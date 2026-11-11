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

/** A due occurrence whose payload could not be offloaded to its project's storage. */
export interface FailedOccurrence {
	workflowId: string;
	occurrenceAt: string;
	error: unknown;
}

/**
 * Persists the per-workflow schedule cursor. Advancing the cursor marks an
 * occurrence as ingested so the next tick does not re-match it.
 */
export interface ScheduleCursorStore {
	advanceMany(entries: Array<{ workflowId: string; occurrenceAt: string }>): Promise<void>;
	/**
	 * Records occurrences as failed and advances their cursors past them, so
	 * they surface as failed runs instead of being re-matched or dropped.
	 */
	markFailed(entries: FailedOccurrence[]): Promise<void>;
}

/** Enqueues execution jobs for pickup by queue workers. */
export interface ScheduleJobQueue {
	enqueueBatch(batchId: string, jobs: ScheduleJob[]): Promise<void>;
}

export interface ScheduleIngestDeps {
	artifactManager: S3BinaryDataManager;
	cursorStore: ScheduleCursorStore;
	jobQueue: ScheduleJobQueue;
}

/**
 * Ingests due scheduled-workflow occurrences: offloads each occurrence's
 * trigger payload to per-project object storage, enqueues the execution
 * jobs, and advances the schedule cursors.
 *
 * The tick's successful offloads are enqueued and their cursors advanced as
 * one batch; an occurrence whose upload failed is marked failed on its own,
 * so one project's storage problem cannot stall every project sharing the
 * tick. Batching replaced per-occurrence ingest, which needed one storage
 * round-trip and two DB writes per occurrence and dominated tick latency on
 * instances with thousands of active schedules.
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
	 * Cursors advance only for occurrences whose payload is in object storage
	 * and whose job is enqueued. Occurrences whose upload failed are marked
	 * failed rather than re-matched, never silently skipped. Failures of the
	 * shared enqueue or cursor writes still fail the tick so the queue
	 * redelivers it.
	 */
	async ingestTick(tick: ScheduleTick) {
		const { cursorStore, jobQueue } = this.requireDeps();
		const { tickId, dueOccurrences } = tick;

		if (dueOccurrences.length === 0) return;

		this.logger.info(
			`Schedule tick ${tickId}: matched ${dueOccurrences.length} due workflow(s) across ${this.projectCount(dueOccurrences)} project(s), enqueueing batch`,
		);

		const batchId = `${tickId}-b1`;
		let jobs: ScheduleJob[] = [];
		let failed: FailedOccurrence[] = [];

		try {
			({ jobs, failed } = await this.offloadBatch(batchId, dueOccurrences));
			if (jobs.length > 0) {
				await jobQueue.enqueueBatch(batchId, jobs);
				await cursorStore.advanceMany(
					jobs.map(({ workflowId, occurrenceAt }) => ({ workflowId, occurrenceAt })),
				);
			}
			if (failed.length > 0) {
				await cursorStore.markFailed(failed);
			}
		} catch (error) {
			this.logger.warn(
				`Schedule tick ${tickId}: cursor not advanced for ${dueOccurrences.length} due workflow(s) — batch ingest did not commit, occurrences will re-match next tick`,
			);
			throw error;
		}

		if (failed.length > 0) {
			this.logger.warn(
				`Schedule tick ${tickId}: marked ${failed.length} due workflow(s) failed after payload offload failed — cause redacted (may contain tenant data)`,
			);
		}

		this.logger.info(
			`Enqueued execution batch ${batchId} (${jobs.length} jobs) — offloaded payloads and advanced schedule cursors`,
		);
	}

	/**
	 * Offload every occurrence's payload to its project's artifact bucket.
	 *
	 * Uploads run concurrently — payload offload is I/O bound and the
	 * batch's wall time is otherwise the sum of every project's storage
	 * latency. Uploads settle independently: only occurrences whose payload
	 * was stored become jobs, so the queue never receives dangling payload
	 * references, and a failed upload is returned instead of failing the
	 * rest of the batch.
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
	private async offloadBatch(batchId: string, occurrences: DueOccurrence[]) {
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
		results.forEach((result, i) => {
			if (result.status === 'fulfilled') {
				jobs.push(result.value);
			} else {
				const { workflowId, occurrenceAt } = occurrences[i];
				failed.push({ workflowId, occurrenceAt, error: result.reason });
			}
		});

		return { jobs, failed };
	}

	private projectCount(occurrences: DueOccurrence[]) {
		return new Set(occurrences.map((o) => o.projectSlug)).size;
	}

	private requireDeps(): ScheduleIngestDeps {
		if (!this.deps) {
			throw new UnexpectedError('ScheduleIngestService used before init()');
		}
		return this.deps;
	}
}
