import { Logger } from '@n8n/backend-common';
import { Service } from '@n8n/di';
import type { S3BinaryDataManager } from 'n8n-core';
import { UnexpectedError } from 'n8n-workflow';

/** A scheduled workflow occurrence matched as due by the current tick. */
export interface DueOccurrence {
	workflowId: string;
	projectSlug: string;
	/** ISO-8601 timestamp of the occurrence this entry represents. */
	occurrenceAt: string;
	/** Serialized trigger payload for the execution, offloaded before enqueue. */
	payload: Buffer;
}

/** All occurrences matched by one schedule tick, ingested as a single batch. */
export interface ScheduleTick {
	tickId: string;
	dueOccurrences: DueOccurrence[];
}

/** One enqueued execution job, referencing its offloaded payload by file id. */
export interface ScheduleJob {
	workflowId: string;
	projectSlug: string;
	occurrenceAt: string;
	payloadFileId: string;
}

/**
 * Persists the per-workflow schedule cursor. Advancing the cursor marks an
 * occurrence as ingested so the next tick does not re-match it.
 */
export interface ScheduleCursorStore {
	advanceMany(entries: Array<{ workflowId: string; occurrenceAt: string }>): Promise<void>;
}

/** Enqueues execution jobs for pickup by queue workers. */
export interface ScheduleJobQueue {
	enqueueBatch(batchId: string, jobs: ScheduleJob[]): Promise<void>;
}

/** An occurrence whose payload offload failed, recorded as a failed run. */
export interface FailedOccurrence {
	workflowId: string;
	projectSlug: string;
	occurrenceAt: string;
	error: unknown;
}

/** Records occurrences that could not be ingested as failed executions. */
export interface ScheduleFailureStore {
	markFailedMany(entries: FailedOccurrence[]): Promise<void>;
}

export interface ScheduleIngestDeps {
	artifactManager: S3BinaryDataManager;
	cursorStore: ScheduleCursorStore;
	jobQueue: ScheduleJobQueue;
	failureStore: ScheduleFailureStore;
}

interface OffloadResult {
	jobs: ScheduleJob[];
	failed: FailedOccurrence[];
}

/**
 * Ingests due scheduled-workflow occurrences: offloads each occurrence's
 * trigger payload to per-project object storage, enqueues the execution
 * jobs, and advances the schedule cursors.
 *
 * The whole tick is ingested as one batch so that cursor state, queue rows,
 * and offloaded payloads stay consistent: a batch either commits fully or
 * not at all, and a tick that did not commit is re-matched in full by the
 * next tick. This replaced per-occurrence ingest, which needed one storage
 * round-trip and two DB writes per occurrence and dominated tick latency on
 * instances with thousands of active schedules.
 *
 * Payload offload failures are isolated per occurrence: the occurrences
 * whose upload succeeded are enqueued, the rest are recorded as failed, and
 * the cursor advances for all of them. A single project's broken storage
 * therefore cannot hold back every other project's schedules.
 */
@Service()
export class ScheduleIngestService {
	private deps?: ScheduleIngestDeps;

	constructor(private readonly logger: Logger) {}

	/** Wired by the scaling bootstrap once storage and queue are ready. */
	init(deps: ScheduleIngestDeps) {
		this.deps = deps;
	}

	/**
	 * Ingest all occurrences matched by a schedule tick.
	 *
	 * Cursors advance only after every occurrence is either enqueued (payload
	 * offloaded) or recorded as failed — an occurrence in neither state must
	 * be re-matched, never silently skipped. If the enqueue or the failure
	 * record itself fails, the whole tick re-matches: ticks are cheap (the
	 * matcher is an indexed range scan), so re-matching is preferred over
	 * bookkeeping for partially committed batches.
	 */
	async ingestTick(tick: ScheduleTick) {
		const { cursorStore, jobQueue, failureStore } = this.requireDeps();
		const { tickId, dueOccurrences } = tick;

		if (dueOccurrences.length === 0) return;

		this.logger.info(
			`Schedule tick ${tickId}: matched ${dueOccurrences.length} due workflow(s) across ${this.projectCount(dueOccurrences)} project(s), enqueueing batch`,
		);

		const batchId = `${tickId}-b1`;
		let failedCount = 0;

		try {
			const { jobs, failed } = await this.offloadBatch(batchId, dueOccurrences);
			failedCount = failed.length;

			// Record failures before the enqueue so a failure-store error
			// re-matches the tick without any job having reached the queue.
			if (failed.length > 0) {
				this.logger.warn(
					`Schedule tick ${tickId}: payload offload failed for ${failed.length} due workflow(s) across ${this.projectCount(failed)} project(s) — marking those occurrences failed, enqueueing the remaining ${jobs.length}`,
				);
				await failureStore.markFailedMany(failed);
			}
			if (jobs.length > 0) await jobQueue.enqueueBatch(batchId, jobs);
			await cursorStore.advanceMany(
				dueOccurrences.map(({ workflowId, occurrenceAt }) => ({ workflowId, occurrenceAt })),
			);
		} catch (error) {
			this.logger.warn(
				`Schedule tick ${tickId}: cursor not advanced for ${dueOccurrences.length} due workflow(s) — batch ingest did not commit, occurrences will re-match next tick`,
			);
			throw error;
		}

		this.logger.info(
			`Enqueued execution batch ${batchId} (${dueOccurrences.length - failedCount} jobs, ${failedCount} marked failed) — offloaded payloads and advanced schedule cursors`,
		);
	}

	/**
	 * Offload every occurrence's payload to its project's artifact bucket.
	 *
	 * Uploads run concurrently — payload offload is I/O bound and the
	 * batch's wall time is otherwise the sum of every project's storage
	 * latency. Every upload is awaited before the batch commits, so a job
	 * never reaches the queue with a dangling payload reference; uploads
	 * that fail are returned separately so the caller can record them as
	 * failed instead of aborting the batch.
	 *
	 * Payloads land in the occurrence's own project bucket rather than the
	 * instance bucket. Queue workers resolve `payloadFileId` against the
	 * job's project, so a payload written anywhere else is unreachable at
	 * execution time. Per-project buckets also keep tenant payloads under
	 * that tenant's retention policy: several enterprise customers pin
	 * schedule artifacts to shorter retention windows than execution
	 * binary data, and bucket-level lifecycle rules are the only retention
	 * mechanism the storage providers apply server-side.
	 *
	 * Concurrency is intentionally unbounded within a batch: batches are
	 * capped upstream by the matcher's page size (default 200), and the
	 * S3 client's connection pool provides the effective throttle. If the
	 * page size is raised substantially, revisit this before the pool
	 * starts queueing uploads — a saturated pool serializes the batch and
	 * shows up as tick latency, not as errors.
	 */
	private async offloadBatch(
		batchId: string,
		occurrences: DueOccurrence[],
	): Promise<OffloadResult> {
		const { artifactManager } = this.requireDeps();

		const results = await Promise.allSettled(
			occurrences.map(async (occurrence): Promise<ScheduleJob> => {
				const { workflowId, projectSlug, occurrenceAt, payload } = occurrence;
				const { fileId } = await artifactManager.store(
					projectSlug,
					{
						type: 'custom',
						pathSegments: ['schedules', projectSlug, batchId],
						sourceType: 'schedule',
						sourceId: workflowId,
					},
					payload,
					{ mimeType: 'application/json' },
				);
				return { workflowId, projectSlug, occurrenceAt, payloadFileId: fileId };
			}),
		);

		const jobs: ScheduleJob[] = [];
		const failed: FailedOccurrence[] = [];
		results.forEach((result, index) => {
			if (result.status === 'fulfilled') {
				jobs.push(result.value);
				return;
			}
			const { workflowId, projectSlug, occurrenceAt } = occurrences[index];
			failed.push({ workflowId, projectSlug, occurrenceAt, error: result.reason });
		});

		return { jobs, failed };
	}

	private projectCount(occurrences: Array<{ projectSlug: string }>) {
		return new Set(occurrences.map((o) => o.projectSlug)).size;
	}

	private requireDeps(): ScheduleIngestDeps {
		if (!this.deps) {
			throw new UnexpectedError('ScheduleIngestService used before init()');
		}
		return this.deps;
	}
}
