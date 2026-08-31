import { Logger } from '@n8n/backend-common';
import { Service } from '@n8n/di';

import type { ScheduleTick } from './schedule-ingest.service';
import { ScheduleIngestService } from './schedule-ingest.service';

/** One schedule tick's batch ingest, carried as a queue job. */
export interface ScheduleIngestJobData {
	jobId: string;
	tick: ScheduleTick;
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
	async run({ jobId, tick }: ScheduleIngestJobData) {
		try {
			await this.scheduleIngest.ingestTick(tick);
		} catch (error) {
			this.logger.error(this.describeFailure(jobId, tick, error));
			throw error;
		}
	}

	private describeFailure(jobId: string, tick: ScheduleTick, error: unknown) {
		const projects = [...new Set(tick.dueOccurrences.map((o) => o.projectSlug))].join(', ');
		const cause =
			error instanceof Error
				? `${error.name}: ${error.message} (project: ${projects}) ${this.flattenStack(error)}`
				: `${String(error)} (project: ${projects})`;
		return `Schedule ingest job ${jobId} (tick ${tick.tickId}) failed, returning to queue: ${cause}`;
	}

	/** The log pipeline is line-oriented: collapse the stack onto one line. */
	private flattenStack(error: Error) {
		return (error.stack ?? '')
			.split('\n')
			.slice(1)
			.map((frame) => frame.trim())
			.join(' ');
	}
}
