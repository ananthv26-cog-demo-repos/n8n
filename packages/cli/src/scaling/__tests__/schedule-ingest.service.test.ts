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
		failureStore.markFailed.mockResolvedValue();
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
		expect(failureStore.markFailed).not.toHaveBeenCalled();
	});

	it('does nothing for a tick with no due occurrences', async () => {
		await service.ingestTick({ tickId: 'tick-2', dueOccurrences: [] });

		expect(artifactManager.store).not.toHaveBeenCalled();
		expect(jobQueue.enqueueBatch).not.toHaveBeenCalled();
		expect(cursorStore.advanceMany).not.toHaveBeenCalled();
	});

	it('marks only the occurrences whose offload failed and enqueues the rest', async () => {
		const uploadError = new Error('InvalidBucketName');
		artifactManager.store
			.mockResolvedValueOnce({ fileId: 'file-1', fileSize: 2 })
			.mockRejectedValueOnce(uploadError)
			.mockResolvedValueOnce({ fileId: 'file-3', fileSize: 2 });

		await service.ingestTick({
			tickId: 'tick-3',
			dueOccurrences: [
				occurrence(),
				occurrence({ workflowId: 'wf-2', projectSlug: 'prj_broken' }),
				occurrence({ workflowId: 'wf-3', projectSlug: 'prj-gamma' }),
			],
		});

		expect(failureStore.markFailed).toHaveBeenCalledWith('tick-3', [
			{
				workflowId: 'wf-2',
				projectSlug: 'prj_broken',
				occurrenceAt: '2025-06-01T00:00:00.000Z',
				error: uploadError,
			},
		]);
		expect(jobQueue.enqueueBatch).toHaveBeenCalledWith('tick-3-b1', [
			expect.objectContaining({ workflowId: 'wf-1', payloadFileId: 'file-1' }),
			expect.objectContaining({ workflowId: 'wf-3', payloadFileId: 'file-3' }),
		]);
		expect(cursorStore.advanceMany).toHaveBeenCalledWith([
			{ workflowId: 'wf-1', occurrenceAt: '2025-06-01T00:00:00.000Z' },
			{ workflowId: 'wf-2', occurrenceAt: '2025-06-01T00:00:00.000Z' },
			{ workflowId: 'wf-3', occurrenceAt: '2025-06-01T00:00:00.000Z' },
		]);
		expect(failureStore.markFailed.mock.invocationCallOrder[0]).toBeLessThan(
			jobQueue.enqueueBatch.mock.invocationCallOrder[0],
		);
	});

	it('keeps tenant identifiers and causes out of the failure log', async () => {
		artifactManager.store.mockRejectedValueOnce(
			new Error('InvalidBucketName: flowforge-artifacts-prj_broken'),
		);

		await service.ingestTick({
			tickId: 'tick-4',
			dueOccurrences: [occurrence({ projectSlug: 'prj_broken' })],
		});

		const logged = vi
			.mocked(logger.warn)
			.mock.calls.map(([message]) => message)
			.join('\n');
		expect(logged).toContain('wf-1');
		expect(logged).not.toContain('prj_broken');
		expect(logged).not.toContain('InvalidBucketName');
	});

	it('advances cursors without enqueueing when every offload fails', async () => {
		artifactManager.store.mockRejectedValue(new Error('upload failed'));

		await service.ingestTick({
			tickId: 'tick-5',
			dueOccurrences: [occurrence(), occurrence({ workflowId: 'wf-2' })],
		});

		expect(failureStore.markFailed).toHaveBeenCalledWith(
			'tick-5',
			expect.arrayContaining([
				expect.objectContaining({ workflowId: 'wf-1' }),
				expect.objectContaining({ workflowId: 'wf-2' }),
			]),
		);
		expect(jobQueue.enqueueBatch).not.toHaveBeenCalled();
		expect(cursorStore.advanceMany).toHaveBeenCalledTimes(1);
	});

	it('does not enqueue or advance cursors when failures cannot be recorded', async () => {
		artifactManager.store
			.mockResolvedValueOnce({ fileId: 'file-1', fileSize: 2 })
			.mockRejectedValueOnce(new Error('upload failed'));
		failureStore.markFailed.mockRejectedValueOnce(new Error('db unavailable'));

		await expect(
			service.ingestTick({
				tickId: 'tick-6',
				dueOccurrences: [occurrence(), occurrence({ workflowId: 'wf-2' })],
			}),
		).rejects.toThrow('db unavailable');

		expect(jobQueue.enqueueBatch).not.toHaveBeenCalled();
		expect(cursorStore.advanceMany).not.toHaveBeenCalled();
	});

	it('does not advance cursors when enqueue fails', async () => {
		jobQueue.enqueueBatch.mockRejectedValueOnce(new Error('ECONNRESET'));

		await expect(
			service.ingestTick({ tickId: 'tick-7', dueOccurrences: [occurrence()] }),
		).rejects.toThrow('ECONNRESET');

		expect(cursorStore.advanceMany).not.toHaveBeenCalled();
	});

	it('throws when used before init', async () => {
		const uninitialized = new ScheduleIngestService(logger);

		await expect(
			uninitialized.ingestTick({ tickId: 'tick-8', dueOccurrences: [occurrence()] }),
		).rejects.toThrow('before init');
	});
});
