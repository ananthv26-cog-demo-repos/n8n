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
		cursorStore.failMany.mockResolvedValue();
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

	it('marks only the failed occurrence and still ingests the rest', async () => {
		const uploadError = new Error('bucket rejected');
		uploadError.name = 'S3UploadError';
		artifactManager.store
			.mockRejectedValueOnce(uploadError)
			.mockResolvedValueOnce({ fileId: 'file-2', fileSize: 2 });

		await service.ingestTick({
			tickId: 'tick-3',
			dueOccurrences: [
				occurrence({ projectSlug: 'prj_meridianfx_eu' }),
				occurrence({ workflowId: 'wf-2', projectSlug: 'prj-beta' }),
			],
		});

		expect(cursorStore.failMany).toHaveBeenCalledWith([
			{
				workflowId: 'wf-1',
				occurrenceAt: '2025-06-01T00:00:00.000Z',
				errorName: 'S3UploadError',
			},
		]);
		expect(jobQueue.enqueueBatch).toHaveBeenCalledWith('tick-3-b1', [
			expect.objectContaining({ workflowId: 'wf-2', payloadFileId: 'file-2' }),
		]);
		expect(cursorStore.advanceMany).toHaveBeenCalledWith([
			{ workflowId: 'wf-2', occurrenceAt: '2025-06-01T00:00:00.000Z' },
		]);
	});

	it('marks failed occurrences before enqueueing the rest', async () => {
		const order: string[] = [];
		cursorStore.failMany.mockImplementation(async () => {
			order.push('failMany');
		});
		jobQueue.enqueueBatch.mockImplementation(async () => {
			order.push('enqueueBatch');
		});
		artifactManager.store
			.mockRejectedValueOnce(new Error('upload failed'))
			.mockResolvedValueOnce({ fileId: 'file-2', fileSize: 2 });

		await service.ingestTick({
			tickId: 'tick-5',
			dueOccurrences: [occurrence(), occurrence({ workflowId: 'wf-2' })],
		});

		expect(order).toEqual(['failMany', 'enqueueBatch']);
	});

	it('enqueues nothing when every offload fails', async () => {
		artifactManager.store.mockRejectedValue(new Error('upload failed'));

		await service.ingestTick({
			tickId: 'tick-6',
			dueOccurrences: [occurrence(), occurrence({ workflowId: 'wf-2' })],
		});

		expect(cursorStore.failMany).toHaveBeenCalledWith([
			expect.objectContaining({ workflowId: 'wf-1' }),
			expect.objectContaining({ workflowId: 'wf-2' }),
		]);
		expect(jobQueue.enqueueBatch).not.toHaveBeenCalled();
		expect(cursorStore.advanceMany).not.toHaveBeenCalled();
	});

	it('rethrows and leaves cursors unadvanced when the enqueue fails', async () => {
		jobQueue.enqueueBatch.mockRejectedValue(new Error('queue unavailable'));

		await expect(
			service.ingestTick({ tickId: 'tick-7', dueOccurrences: [occurrence()] }),
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
