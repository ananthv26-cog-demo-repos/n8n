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

	it('ingests the tick and logs nothing on success', async () => {
		scheduleIngest.ingestTick.mockResolvedValue();

		await runner.run({ jobId: 'job-1', tick });

		expect(scheduleIngest.ingestTick).toHaveBeenCalledWith(tick);
		expect(logger.error).not.toHaveBeenCalled();
	});

	it('logs the failure with its cause and rethrows so the queue redelivers', async () => {
		const error = new Error('upload failed');
		error.name = 'S3UploadError';
		scheduleIngest.ingestTick.mockRejectedValue(error);

		await expect(runner.run({ jobId: 'job-1', tick })).rejects.toThrow(error);

		expect(logger.error).toHaveBeenCalledWith(
			expect.stringContaining(
				'Schedule ingest job job-1 (tick tick-1) failed, returning to queue: S3UploadError: upload failed (project: prj-alpha)',
			),
		);
	});
});
