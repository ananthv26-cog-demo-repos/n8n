import { mockLogger } from '@n8n/backend-test-utils';
import type { S3BinaryDataManager } from 'n8n-core';
import { mock } from 'vitest-mock-extended';

import type {
	DueOccurrence,
	ScheduleCursorStore,
	ScheduleFailureStore,
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
	const failureStore = mock<ScheduleFailureStore>();

	let service: ScheduleIngestService;

	beforeEach(() => {
		vi.clearAllMocks();
		service = new ScheduleIngestService(logger);
		service.init({ artifactManager, cursorStore, jobQueue, failureStore });
		artifactManager.store.mockResolvedValue({ fileId: 'file-1', fileSize: 2 });
		cursorStore.advanceMany.mockResolvedValue();
		jobQueue.enqueueBatch.mockResolvedValue();
		failureStore.markFailedMany.mockResolvedValue();
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
		expect(failureStore.markFailedMany).not.toHaveBeenCalled();
	});

	it('does nothing for a tick with no due occurrences', async () => {
		await service.ingestTick({ tickId: 'tick-2', dueOccurrences: [] });

		expect(artifactManager.store).not.toHaveBeenCalled();
		expect(jobQueue.enqueueBatch).not.toHaveBeenCalled();
		expect(cursorStore.advanceMany).not.toHaveBeenCalled();
	});

	it('marks only the occurrences whose offload failed and enqueues the rest', async () => {
		const error = new Error('upload failed');
		artifactManager.store.mockImplementation(async (projectSlug) => {
			if (projectSlug === 'prj_broken') throw error;
			return { fileId: 'file-1', fileSize: 2 };
		});

		await service.ingestTick({
			tickId: 'tick-3',
			dueOccurrences: [
				occurrence(),
				occurrence({ workflowId: 'wf-2', projectSlug: 'prj_broken' }),
				occurrence({ workflowId: 'wf-3', projectSlug: 'prj-beta' }),
			],
		});

		expect(jobQueue.enqueueBatch).toHaveBeenCalledWith('tick-3-b1', [
			expect.objectContaining({ workflowId: 'wf-1' }),
			expect.objectContaining({ workflowId: 'wf-3' }),
		]);
		expect(failureStore.markFailedMany).toHaveBeenCalledWith([
			{
				workflowId: 'wf-2',
				projectSlug: 'prj_broken',
				occurrenceAt: '2025-06-01T00:00:00.000Z',
				error,
			},
		]);
		expect(cursorStore.advanceMany).toHaveBeenCalledWith([
			{ workflowId: 'wf-1', occurrenceAt: '2025-06-01T00:00:00.000Z' },
			{ workflowId: 'wf-2', occurrenceAt: '2025-06-01T00:00:00.000Z' },
			{ workflowId: 'wf-3', occurrenceAt: '2025-06-01T00:00:00.000Z' },
		]);
		const warning = vi
			.mocked(logger.warn)
			.mock.calls.map(([message]) => message)
			.join('\n');
		expect(warning).toContain('payload offload failed for 1 due workflow(s) across 1 project(s)');
		expect(warning).not.toContain('prj_broken');
		expect(warning).not.toContain(error.message);
	});

	it('skips the enqueue but still marks failed and advances cursors when every offload fails', async () => {
		artifactManager.store.mockRejectedValue(new Error('upload failed'));

		await service.ingestTick({
			tickId: 'tick-3b',
			dueOccurrences: [occurrence(), occurrence({ workflowId: 'wf-2' })],
		});

		expect(jobQueue.enqueueBatch).not.toHaveBeenCalled();
		expect(failureStore.markFailedMany).toHaveBeenCalledWith([
			expect.objectContaining({ workflowId: 'wf-1' }),
			expect.objectContaining({ workflowId: 'wf-2' }),
		]);
		expect(cursorStore.advanceMany).toHaveBeenCalledTimes(1);
	});

	it('does not enqueue or advance cursors when recording the failures fails', async () => {
		artifactManager.store
			.mockResolvedValueOnce({ fileId: 'file-1', fileSize: 2 })
			.mockRejectedValueOnce(new Error('upload failed'));
		failureStore.markFailedMany.mockRejectedValue(new Error('db down'));

		await expect(
			service.ingestTick({
				tickId: 'tick-3c',
				dueOccurrences: [occurrence(), occurrence({ workflowId: 'wf-2' })],
			}),
		).rejects.toThrow('db down');

		expect(jobQueue.enqueueBatch).not.toHaveBeenCalled();
		expect(cursorStore.advanceMany).not.toHaveBeenCalled();
	});

	it('does not advance cursors when the enqueue fails', async () => {
		jobQueue.enqueueBatch.mockRejectedValue(new Error('queue down'));

		await expect(
			service.ingestTick({ tickId: 'tick-3d', dueOccurrences: [occurrence()] }),
		).rejects.toThrow('queue down');

		expect(cursorStore.advanceMany).not.toHaveBeenCalled();
	});

	it('throws when used before init', async () => {
		const uninitialized = new ScheduleIngestService(logger);

		await expect(
			uninitialized.ingestTick({ tickId: 'tick-4', dueOccurrences: [occurrence()] }),
		).rejects.toThrow('before init');
	});
});
