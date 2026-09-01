import { Logger } from '@n8n/backend-common';
import { Service } from '@n8n/di';

import type { ScheduleTick } from './schedule-ingest.service';
import { ScheduleIngestService } from './schedule-ingest.service';

/** One schedule tick's batch ingest, carried as a queue job. */
export interface ScheduleIngestJobData {
	jobId: string;
	tick: ScheduleTick;
}

/** Receive metadata the queue hands the worker with each delivery. */
export interface ScheduleIngestReceipt {
	attempt: number;
	maxReceiveCount: number;
}

/**
 * Runs schedule tick ingest as a redeliverable queue job.
 *
 * The scheduler enqueues each tick instead of ingesting inline so an ingest
 * interrupted by a crash or failed by a dependency is redelivered per the
 * queue's redrive policy and dead-letters once receives are exhausted.
 * Registered by the scaling bootstrap alongside the ingest service.
 */
@Service()
export class ScheduleIngestJobRunner {
	constructor(
		private readonly logger: Logger,
		private readonly scheduleIngest: ScheduleIngestService,
	) {}

	/**
	 * Process one schedule-ingest job. Rethrows on failure so the queue
	 * redelivers the job — a tick whose ingest did not commit must be
	 * retried, never dropped.
	 */
	async run({ jobId, tick }: ScheduleIngestJobData, receipt: ScheduleIngestReceipt) {
		try {
			await this.scheduleIngest.ingestTick(tick);
		} catch (error) {
			this.logger.error(this.describeFailure(jobId, tick, receipt));
			throw error;
		}
	}

	/**
	 * Failure causes from artifact offload can embed tenant resource
	 * identifiers, and worker logs ship to the shared pipeline — withhold
	 * the cause entirely and keep the delivery attempt for debuggability.
	 */
	private describeFailure(
		jobId: string,
		tick: ScheduleTick,
		{ attempt, maxReceiveCount }: ScheduleIngestReceipt,
	) {
		return `Schedule ingest job ${jobId} (tick ${tick.tickId}) failed on attempt ${attempt}/${maxReceiveCount}, returning to queue — cause redacted (may contain tenant data)`;
	}
}
