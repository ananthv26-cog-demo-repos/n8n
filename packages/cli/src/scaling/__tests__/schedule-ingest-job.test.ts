import { mockLogger } from '@n8n/backend-test-utils';
import { mock } from 'vitest-mock-extended';

import { ScheduleIngestJobRunner } from '../schedule-ingest-job';
import type { ScheduleIngestService, ScheduleTick } from '../schedule-ingest.service';

const tick: ScheduleTick = {
	tickId: 'tick-1',
	dueOccurrences: [
		{
			workflowId: 'wf-1',
			projectSlug: 'prj-alpha',
			occurrenceAt: '2025-06-01T00:00:00.000Z',
			payload: Buffer.from('{}'),
		},
	],
};

describe('ScheduleIngestJobRunner', () => {
	const logger = mockLogger();
	const scheduleIngest = mock<ScheduleIngestService>();

	let runner: ScheduleIngestJobRunner;

	beforeEach(() => {
		vi.clearAllMocks();
		runner = new ScheduleIngestJobRunner(logger, scheduleIngest);
	});

	const receipt = { attempt: 3, maxReceiveCount: 8 };

	it('ingests the tick and logs nothing on success', async () => {
		scheduleIngest.ingestTick.mockResolvedValue();

		await runner.run({ jobId: 'job-1', tick }, receipt);

		expect(scheduleIngest.ingestTick).toHaveBeenCalledWith(tick);
		expect(logger.error).not.toHaveBeenCalled();
	});

	it('logs the failure with the cause withheld and rethrows so the queue redelivers', async () => {
		const error = new Error('ProviderError: request rejected by storage backend');
		error.name = 'S3UploadError';
		scheduleIngest.ingestTick.mockRejectedValue(error);

		await expect(runner.run({ jobId: 'job-1', tick }, receipt)).rejects.toThrow(error);

		expect(logger.error).toHaveBeenCalledWith(
			'Schedule ingest job job-1 (tick tick-1) failed on attempt 3/8, returning to queue — cause redacted (may contain tenant data)',
		);
		const [message] = vi.mocked(logger.error).mock.calls[0];
		expect(message).not.toContain(error.message);
		expect(message).not.toContain(tick.dueOccurrences[0].projectSlug);
	});
});
