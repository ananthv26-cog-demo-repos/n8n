import { mockLogger } from '@n8n/backend-test-utils';
import type { S3BinaryDataManager } from 'n8n-core';
import { mock } from 'vitest-mock-extended';

import type {
	DueOccurrence,
	ScheduleCursorStore,
	ScheduleJobQueue,
} from '../schedule-ingest.service';
import { ScheduleIngestService } from '../schedule-ingest.service';

const occurrence = (overrides: Partial<DueOccurrence> = {}): DueOccurrence => ({
	workflowId: 'wf-1',
	projectSlug: 'prj-alpha',
	occurrenceAt: '2025-06-01T00:00:00.000Z',
	payload: Buffer.from('{}'),
	...overrides,
});

describe('ScheduleIngestService', () => {
	const logger = mockLogger();
	const artifactManager = mock<S3BinaryDataManager>();
	const cursorStore = mock<ScheduleCursorStore>();
	const jobQueue = mock<ScheduleJobQueue>();

	let service: ScheduleIngestService;

	beforeEach(() => {
		vi.clearAllMocks();
		service = new ScheduleIngestService(logger);
		service.init({ artifactManager, cursorStore, jobQueue });
		artifactManager.store.mockResolvedValue({ fileId: 'file-1', fileSize: 2 });
		cursorStore.advanceMany.mockResolvedValue();
		jobQueue.enqueueBatch.mockResolvedValue();
	});

	it('offloads every occurrence, enqueues the batch, and advances cursors', async () => {
		const occurrences = [occurrence(), occurrence({ workflowId: 'wf-2', projectSlug: 'prj-beta' })];

		await service.ingestTick({ tickId: 'tick-1', dueOccurrences: occurrences });

		expect(artifactManager.store).toHaveBeenCalledTimes(2);
		expect(jobQueue.enqueueBatch).toHaveBeenCalledWith('tick-1-b1', [
			expect.objectContaining({ workflowId: 'wf-1', payloadFileId: 'file-1' }),
			expect.objectContaining({ workflowId: 'wf-2', payloadFileId: 'file-1' }),
		]);
		expect(cursorStore.advanceMany).toHaveBeenCalledWith([
			{ workflowId: 'wf-1', occurrenceAt: '2025-06-01T00:00:00.000Z' },
			{ workflowId: 'wf-2', occurrenceAt: '2025-06-01T00:00:00.000Z' },
		]);
	});

	it('does nothing for a tick with no due occurrences', async () => {
		await service.ingestTick({ tickId: 'tick-2', dueOccurrences: [] });

		expect(artifactManager.store).not.toHaveBeenCalled();
		expect(jobQueue.enqueueBatch).not.toHaveBeenCalled();
		expect(cursorStore.advanceMany).not.toHaveBeenCalled();
	});

	it('marks only the occurrence whose offload failed and commits the rest of the tick', async () => {
		const uploadError = new Error('InvalidBucketName: flowforge-artifacts-prj_bad');
		uploadError.name = 'S3UploadError';
		artifactManager.store
			.mockResolvedValueOnce({ fileId: 'file-1', fileSize: 2 })
			.mockRejectedValueOnce(uploadError)
			.mockResolvedValueOnce({ fileId: 'file-3', fileSize: 2 });

		const result = await service.ingestTick({
			tickId: 'tick-3',
			dueOccurrences: [
				occurrence(),
				occurrence({ workflowId: 'wf-2', projectSlug: 'prj_bad' }),
				occurrence({ workflowId: 'wf-3', projectSlug: 'prj-gamma' }),
			],
		});

		expect(jobQueue.enqueueBatch).toHaveBeenCalledWith('tick-3-b1', [
			expect.objectContaining({ workflowId: 'wf-1', payloadFileId: 'file-1' }),
			expect.objectContaining({ workflowId: 'wf-3', payloadFileId: 'file-3' }),
		]);
		expect(cursorStore.advanceMany).toHaveBeenCalledWith([
			{ workflowId: 'wf-1', occurrenceAt: '2025-06-01T00:00:00.000Z' },
			{ workflowId: 'wf-3', occurrenceAt: '2025-06-01T00:00:00.000Z' },
		]);
		expect(result.enqueued).toHaveLength(2);
		expect(result.failed).toEqual([
			{
				workflowId: 'wf-2',
				projectSlug: 'prj_bad',
				occurrenceAt: '2025-06-01T00:00:00.000Z',
				error: uploadError,
			},
		]);

		const warning = vi
			.mocked(logger.warn)
			.mock.calls.map(([message]) => String(message))
			.find((message) => message.includes('payload offload failed'));
		expect(warning).toContain('1 of 3 due workflow(s) (S3UploadError x1)');
		expect(warning).not.toContain(uploadError.message);
		expect(warning).not.toContain('prj_bad');
	});

	it('does not enqueue or advance cursors when every offload fails, without failing the tick', async () => {
		artifactManager.store.mockRejectedValue(new Error('upload failed'));

		const result = await service.ingestTick({
			tickId: 'tick-5',
			dueOccurrences: [occurrence(), occurrence({ workflowId: 'wf-2' })],
		});

		expect(result.failed).toHaveLength(2);
		expect(jobQueue.enqueueBatch).not.toHaveBeenCalled();
		expect(cursorStore.advanceMany).not.toHaveBeenCalled();
	});

	it('fails the whole tick without advancing cursors when enqueue fails', async () => {
		jobQueue.enqueueBatch.mockRejectedValue(new Error('queue unavailable'));

		await expect(
			service.ingestTick({
				tickId: 'tick-6',
				dueOccurrences: [occurrence(), occurrence({ workflowId: 'wf-2' })],
			}),
		).rejects.toThrow('queue unavailable');

		expect(cursorStore.advanceMany).not.toHaveBeenCalled();
	});

	it('throws when used before init', async () => {
		const uninitialized = new ScheduleIngestService(logger);

		await expect(
			uninitialized.ingestTick({ tickId: 'tick-4', dueOccurrences: [occurrence()] }),
		).rejects.toThrow('before init');
	});
});
