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

	/**
	 * Retire occurrences whose ingest failed. Implementations must record
	 * the failure durably and advance past the occurrence, so a retired
	 * occurrence is neither re-matched by a later tick nor left pending —
	 * otherwise a poisoned occurrence keeps returning. `errorName` is the
	 * failure's error class, kept free of tenant identifiers.
	 */
	failMany(
		entries: Array<{ workflowId: string; occurrenceAt: string; errorName: string }>,
	): Promise<void>;
}

/** Enqueues execution jobs for pickup by queue workers. */
export interface ScheduleJobQueue {
	enqueueBatch(batchId: string, jobs: ScheduleJob[]): Promise<void>;
}

/** One occurrence whose payload offload failed, with its error class. */
interface OffloadFailure {
	occurrence: DueOccurrence;
	errorName: string;
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
 * The tick is ingested as one batch so that cursor state, queue rows, and
 * offloaded payloads stay consistent. This replaced per-occurrence ingest,
 * which needed one storage round-trip and two DB writes per occurrence and
 * dominated tick latency on instances with thousands of active schedules.
 *
 * An occurrence whose payload offload fails is retired as failed and the
 * rest of the batch still commits: one project's storage error must not
 * hold back every other project's schedules.
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
	 * Occurrences whose payload offload failed go to `cursorStore.failMany`
	 * before anything else — first so a later error in this job does not
	 * retry them on redelivery, and retired from re-matching per that
	 * method's contract. The remaining occurrences are then enqueued and
	 * their cursors advanced. An occurrence is therefore never left pending
	 * with its payload missing from object storage, and never silently
	 * skipped either.
	 *
	 * Enqueue and cursor advance are still two sequential writes for the
	 * occurrences that did offload: if either throws the job is retried and
	 * those occurrences re-match, so an advance that fails after a
	 * successful enqueue can enqueue them a second time (unchanged by this
	 * isolation, and why the pair is kept in one step).
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

		if (failures.length > 0) {
			this.logger.error(
				`Schedule tick ${tickId}: payload offload failed for ${failures.length} of ${dueOccurrences.length} due workflow(s) across ${this.projectCount(failures.map((f) => f.occurrence))} project(s) — marking them failed, ingesting the rest (error(s): ${this.describeFailures(failures)})`,
			);
			await cursorStore.failMany(
				failures.map(({ occurrence: { workflowId, occurrenceAt }, errorName }) => ({
					workflowId,
					occurrenceAt,
					errorName,
				})),
			);
		}

		if (jobs.length === 0) return;

		try {
			await jobQueue.enqueueBatch(batchId, jobs);
			await cursorStore.advanceMany(
				jobs.map(({ workflowId, occurrenceAt }) => ({ workflowId, occurrenceAt })),
			);
		} catch (error) {
			this.logger.warn(
				`Schedule tick ${tickId}: cursor not advanced for ${jobs.length} due workflow(s) — batch ingest did not commit, occurrences will re-match next tick`,
			);
			throw error;
		}

		this.logger.info(
			`Enqueued execution batch ${batchId} (${jobs.length} jobs) — offloaded payloads and advanced schedule cursors`,
		);
	}

	/**
	 * Offload every occurrence's payload to its project's artifact bucket,
	 * returning the jobs whose payload landed and the occurrences whose
	 * upload failed.
	 *
	 * Uploads run concurrently — payload offload is I/O bound and the
	 * batch's wall time is otherwise the sum of every project's storage
	 * latency. Every upload is awaited before the batch proceeds, so a job
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
		const failures: OffloadFailure[] = [];

		results.forEach((result, i) => {
			if (result.status === 'fulfilled') {
				jobs.push(result.value);
			} else {
				failures.push({
					occurrence: occurrences[i],
					errorName: this.errorName(result.reason),
				});
			}
		});

		return { jobs, failures };
	}

	/**
	 * Distinct error classes in a batch's failures. Only the error class is
	 * reported: offload failure messages can embed tenant resource
	 * identifiers (bucket names, keys, endpoints).
	 */
	private describeFailures(failures: OffloadFailure[]) {
		return [...new Set(failures.map(({ errorName }) => errorName))].join(', ');
	}

	private errorName(error: unknown) {
		if (error instanceof Error && error.name) return error.name;
		return 'UnknownError';
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
