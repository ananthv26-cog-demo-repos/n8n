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
		cursorStore.markFailed.mockResolvedValue();
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

	it('marks only the failed occurrence failed and ingests the rest of the tick', async () => {
		const uploadError = new Error('upload failed');
		artifactManager.store
			.mockResolvedValueOnce({ fileId: 'file-1', fileSize: 2 })
			.mockRejectedValueOnce(uploadError)
			.mockResolvedValueOnce({ fileId: 'file-3', fileSize: 2 });

		await service.ingestTick({
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
		expect(cursorStore.markFailed).toHaveBeenCalledWith([
			{ workflowId: 'wf-2', occurrenceAt: '2025-06-01T00:00:00.000Z', error: uploadError },
		]);
		expect(logger.warn).toHaveBeenCalledWith(expect.not.stringContaining('prj_bad'));
	});

	it('marks every occurrence failed without enqueueing when all offloads fail', async () => {
		artifactManager.store.mockRejectedValue(new Error('upload failed'));

		await service.ingestTick({
			tickId: 'tick-5',
			dueOccurrences: [occurrence(), occurrence({ workflowId: 'wf-2' })],
		});

		expect(jobQueue.enqueueBatch).not.toHaveBeenCalled();
		expect(cursorStore.advanceMany).not.toHaveBeenCalled();
		expect(cursorStore.markFailed).toHaveBeenCalledWith([
			expect.objectContaining({ workflowId: 'wf-1' }),
			expect.objectContaining({ workflowId: 'wf-2' }),
		]);
	});

	it('fails the tick without advancing cursors when enqueue fails', async () => {
		jobQueue.enqueueBatch.mockRejectedValue(new Error('queue unavailable'));

		await expect(
			service.ingestTick({ tickId: 'tick-6', dueOccurrences: [occurrence()] }),
		).rejects.toThrow('queue unavailable');

		expect(cursorStore.advanceMany).not.toHaveBeenCalled();
		expect(cursorStore.markFailed).not.toHaveBeenCalled();
	});

	it('throws when used before init', async () => {
		const uninitialized = new ScheduleIngestService(logger);

		await expect(
			uninitialized.ingestTick({ tickId: 'tick-4', dueOccurrences: [occurrence()] }),
		).rejects.toThrow('before init');
	});
});
