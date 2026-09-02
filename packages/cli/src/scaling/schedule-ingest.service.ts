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

/** An occurrence whose payload offload failed, to be marked failed instead of enqueued. */
export interface FailedOccurrence {
	workflowId: string;
	projectSlug: string;
	occurrenceAt: string;
	/** Provider failure description, persisted with the failure record. May contain tenant resource identifiers — never log it. */
	reason: string;
}

/**
 * Persists the per-workflow schedule cursor. Advancing the cursor marks an
 * occurrence as ingested so the next tick does not re-match it.
 */
export interface ScheduleCursorStore {
	advanceMany(entries: Array<{ workflowId: string; occurrenceAt: string }>): Promise<void>;

	/**
	 * Records the occurrences as failed and advances their cursors so they
	 * are not re-matched. Used when an occurrence cannot be ingested (e.g.
	 * its payload offload fails) so it surfaces as a failed occurrence
	 * instead of blocking or being retried with the rest of the tick.
	 * Must be idempotent: a redelivered tick may mark the same occurrence
	 * failed again.
	 */
	markFailedMany(entries: FailedOccurrence[]): Promise<void>;
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
 * The tick is ingested as one batch (one enqueue, one cursor write) so that
 * cursor state, queue rows, and offloaded payloads stay consistent. This
 * replaced per-occurrence ingest, which needed one storage round-trip and
 * two DB writes per occurrence and dominated tick latency on instances with
 * thousands of active schedules. Occurrences whose payload offload fails are
 * excluded from the batch and marked failed immediately, so one project's
 * storage failure cannot block the rest of the tick.
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
	 * Occurrences whose payload offload succeeds are enqueued and their
	 * cursors advanced as one batch. Occurrences whose offload fails are
	 * marked failed immediately — their cursors advance with a failure
	 * record, so they surface as failed occurrences rather than re-matching
	 * every tick or blocking other projects' schedules. Enqueue or cursor
	 * failures still rethrow so the queue redelivers the tick.
	 *
	 * Failures are marked before the batch enqueues, so a rejection while
	 * marking failures cannot leave already-enqueued jobs behind for a
	 * redelivered tick to enqueue again; the only write after the enqueue
	 * is the cursor advance for the enqueued jobs.
	 */
	async ingestTick(tick: ScheduleTick) {
		const { cursorStore, jobQueue } = this.requireDeps();
		const { tickId, dueOccurrences } = tick;

		if (dueOccurrences.length === 0) return;

		this.logger.info(
			`Schedule tick ${tickId}: matched ${dueOccurrences.length} due workflow(s) across ${this.projectCount(dueOccurrences)} project(s), enqueueing batch`,
		);

		const batchId = `${tickId}-b1`;

		const { jobs, failures } = await this.offloadBatch(batchId, dueOccurrences);

		let failuresMarked = failures.length === 0;

		try {
			if (failures.length > 0) {
				await cursorStore.markFailedMany(failures);
				failuresMarked = true;
				// Failure reasons can embed tenant resource identifiers — log counts only.
				this.logger.warn(
					`Schedule tick ${tickId}: marked ${failures.length} occurrence(s) across ${this.projectCount(failures)} project(s) as failed — payload offload failed, causes withheld (may contain tenant data)`,
				);
			}
			if (jobs.length > 0) {
				await jobQueue.enqueueBatch(batchId, jobs);
				await cursorStore.advanceMany(
					jobs.map(({ workflowId, occurrenceAt }) => ({ workflowId, occurrenceAt })),
				);
			}
		} catch (error) {
			const remaining = failuresMarked ? jobs.length : dueOccurrences.length;
			this.logger.warn(
				`Schedule tick ${tickId}: cursor not advanced for ${remaining} of ${dueOccurrences.length} due workflow(s) — batch ingest did not commit, those occurrences will re-match next tick`,
			);
			throw error;
		}

		if (jobs.length > 0) {
			this.logger.info(
				`Enqueued execution batch ${batchId} (${jobs.length} jobs) — offloaded payloads and advanced schedule cursors`,
			);
		}
	}

	/**
	 * Offload every occurrence's payload to its project's artifact bucket.
	 *
	 * Uploads run concurrently — payload offload is I/O bound and the
	 * batch's wall time is otherwise the sum of every project's storage
	 * latency. The batch commits only once every upload has succeeded, so
	 * a partial batch never reaches the queue with dangling payload
	 * references.
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
	 *
	 * Failed uploads are collected rather than aborting the batch, so one
	 * occurrence's storage failure does not discard the other uploads or
	 * block their enqueue.
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
		const failures: FailedOccurrence[] = [];

		results.forEach((result, index) => {
			if (result.status === 'fulfilled') {
				jobs.push(result.value);
				return;
			}
			const { workflowId, projectSlug, occurrenceAt } = occurrences[index];
			failures.push({
				workflowId,
				projectSlug,
				occurrenceAt,
				reason: this.describeOffloadFailure(result.reason),
			});
		});

		return { jobs, failures };
	}

	private describeOffloadFailure(reason: unknown) {
		if (reason instanceof Error) return reason.message;
		return 'Unknown payload offload error';
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
