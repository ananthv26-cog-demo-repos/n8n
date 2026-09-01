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
 * occurrence as ingested so the next tick does not re-match it; marking it
 * failed records the failure and likewise stops it from re-matching.
 */
export interface ScheduleCursorStore {
	advanceMany(entries: Array<{ workflowId: string; occurrenceAt: string }>): Promise<void>;

	markFailedMany(
		entries: Array<{ workflowId: string; occurrenceAt: string; reason: string }>,
	): Promise<void>;
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
 * The whole tick is ingested as one batch so that cursor state, queue rows,
 * and offloaded payloads stay consistent. This replaced per-occurrence
 * ingest, which needed one storage round-trip and two DB writes per
 * occurrence and dominated tick latency on instances with thousands of
 * active schedules. Occurrences whose payload offload fails are marked
 * failed and excluded from the batch, so one project's broken storage
 * cannot block the rest of the tick from being enqueued.
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
	 * Occurrences whose payload offload fails are marked failed so they do
	 * not re-match on the next tick; the rest of the batch is enqueued and
	 * has its cursors advanced as usual. The successful jobs are enqueued
	 * and their cursors advanced before the failure records are written, so
	 * a cursor-store error while recording failures never blocks healthy
	 * work — those occurrences just re-match next tick. Cursors advance only
	 * after an occurrence's payload offload and enqueue have both succeeded —
	 * an occurrence whose payload is not in object storage must never be
	 * silently skipped. Enqueue and cursor advancement are not atomic: if
	 * advancement fails after enqueue succeeded, the occurrences re-match
	 * next tick and may be enqueued again.
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

		if (jobs.length > 0) {
			try {
				await jobQueue.enqueueBatch(batchId, jobs);
				await cursorStore.advanceMany(
					jobs.map(({ workflowId, occurrenceAt }) => ({ workflowId, occurrenceAt })),
				);
			} catch (error) {
				this.logger.warn(
					`Schedule tick ${tickId}: cursor not advanced for ${jobs.length} due workflow(s) — occurrences will re-match next tick and may be enqueued again if the batch was already queued`,
				);
				throw error;
			}

			this.logger.info(
				`Enqueued execution batch ${batchId} (${jobs.length} jobs) — offloaded payloads and advanced schedule cursors`,
			);
		}

		if (failures.length > 0) {
			this.logger.warn(
				`Schedule tick ${tickId}: payload offload failed for ${failures.length} of ${dueOccurrences.length} due workflow(s) — marking them failed`,
			);
			try {
				await cursorStore.markFailedMany(
					failures.map(({ occurrence: { workflowId, occurrenceAt }, reason }) => ({
						workflowId,
						occurrenceAt,
						reason,
					})),
				);
			} catch (error) {
				this.logger.warn(
					`Schedule tick ${tickId}: could not persist failure records for ${failures.length} due workflow(s) — they will re-match next tick`,
					{ error },
				);
			}
		}
	}

	/**
	 * Offload every occurrence's payload to its project's artifact bucket.
	 *
	 * Uploads run concurrently — payload offload is I/O bound and the
	 * batch's wall time is otherwise the sum of every project's storage
	 * latency. Failed uploads are collected rather than aborting the batch,
	 * and only occurrences whose upload succeeded become jobs, so a job
	 * never reaches the queue with a dangling payload reference.
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
		const failures: Array<{ occurrence: DueOccurrence; reason: string }> = [];

		results.forEach((result, index) => {
			if (result.status === 'fulfilled') {
				jobs.push(result.value);
				return;
			}
			const occurrence = occurrences[index];
			const reason = result.reason instanceof Error ? result.reason.message : String(result.reason);
			this.logger.warn(
				`Schedule batch ${batchId}: payload offload failed for workflow ${occurrence.workflowId} (project ${occurrence.projectSlug}): ${reason}`,
			);
			failures.push({ occurrence, reason });
		});

		return { jobs, failures };
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
