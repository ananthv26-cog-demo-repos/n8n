import type { Logger } from '@n8n/backend-common';
import { binaryToBuffer } from '@n8n/backend-network';
import { UnexpectedError } from 'n8n-workflow';
import type { Readable } from 'node:stream';
import { v4 as uuid } from 'uuid';

import type { BinaryData } from './types';

/**
 * Thrown when an artifact upload to S3-compatible storage fails. Wraps the
 * provider error so callers see the provider's own error code and message.
 */
export class S3UploadError extends UnexpectedError {
	constructor(message: string, cause?: unknown) {
		super(message, { cause });
	}
}

/**
 * Minimal S3-compatible client surface used for artifact offload. Satisfied
 * by the AWS SDK v3 client via a thin adapter, and by MinIO in self-hosted
 * deployments.
 */
export interface S3ArtifactClient {
	putObject(request: {
		bucket: string;
		key: string;
		body: Buffer;
		contentType?: string;
	}): Promise<void>;

	getObject(request: { bucket: string; key: string }): Promise<Buffer | undefined>;

	deleteObjects(request: { bucket: string; keys: string[] }): Promise<void>;
}

export interface S3ArtifactManagerSettings {
	/**
	 * Base bucket for schedule artifact offload. Per-project buckets are
	 * derived from this so that tenant data stays isolated per bucket and
	 * bucket-level lifecycle/retention policies apply per project.
	 */
	baseBucket: string;
}

/**
 * Offloads schedule-execution artifacts to S3-compatible object storage,
 * one bucket per project.
 *
 * Unlike {@link BinaryDataBlobManager}, which writes everything into the
 * single configured instance bucket, artifact offload isolates each
 * project's payloads in its own derived bucket so per-tenant retention and
 * access policies can be applied at the bucket level.
 */
export class S3BinaryDataManager {
	constructor(
		private readonly client: S3ArtifactClient,
		private readonly settings: S3ArtifactManagerSettings,
		private readonly logger: Logger,
	) {}

	async init() {}

	/**
	 * Derive the artifact bucket for a project from the base bucket and the
	 * project's slug, e.g. `flowforge-artifacts` + `prj_acme` →
	 * `flowforge-artifacts-prj_acme`. Buckets are provisioned out of band by
	 * the deployment tooling when a project is created.
	 */
	bucketForProject(projectSlug: string) {
		return `${this.settings.baseBucket}-${projectSlug}`;
	}

	async store(
		projectSlug: string,
		location: BinaryData.FileLocation,
		bufferOrStream: Buffer | Readable,
		metadata: BinaryData.PreWriteMetadata,
	): Promise<BinaryData.WriteResult> {
		const bucket = this.bucketForProject(projectSlug);
		const fileId = this.toFileId(location);
		const body = await binaryToBuffer(bufferOrStream);

		try {
			await this.client.putObject({ bucket, key: fileId, body, contentType: metadata.mimeType });
		} catch (error) {
			throw new S3UploadError(this.describeProviderError(error), error);
		}

		this.logger.debug('Offloaded schedule artifact to object storage', { bucket, fileId });

		return { fileId, fileSize: body.length };
	}

	async getAsBuffer(projectSlug: string, fileId: string) {
		const bucket = this.bucketForProject(projectSlug);
		const buffer = await this.client.getObject({ bucket, key: fileId });
		if (!buffer) throw new UnexpectedError(`Artifact ${fileId} not found in bucket ${bucket}`);
		return buffer;
	}

	async deleteMany(projectSlug: string, fileIds: string[]) {
		if (fileIds.length === 0) return;
		await this.client.deleteObjects({
			bucket: this.bucketForProject(projectSlug),
			keys: fileIds,
		});
	}

	private toFileId(location: BinaryData.FileLocation) {
		switch (location.type) {
			case 'execution':
				return `workflows/${location.workflowId}/executions/${location.executionId || 'temp'}/binary_data/${uuid()}`;
			case 'custom':
				return `${location.pathSegments.join('/')}/binary_data/${uuid()}`;
		}
	}

	private describeProviderError(error: unknown) {
		if (typeof error === 'object' && error !== null) {
			const { name, message } = error as { name?: string; message?: string };
			if (name && message) return `${name}: ${message}`;
			if (message) return message;
		}
		return 'Unknown object storage error';
	}
}
