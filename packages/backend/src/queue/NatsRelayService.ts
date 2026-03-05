/*
 * SPDX-FileCopyrightText: syuilo and misskey-project
 * SPDX-License-Identifier: AGPL-3.0-only
 */

import { Inject, Injectable, OnApplicationShutdown } from '@nestjs/common';
import { connect, type NatsConnection, type JetStreamClient, type JetStreamManager } from 'nats';
import { DiscardPolicy, RetentionPolicy, StorageType } from 'nats';
import type { Config } from '@/config.js';
import { DI } from '@/di-symbols.js';
import type Logger from '@/logger.js';
import { bindThis } from '@/decorators.js';
import { QueueLoggerService } from './QueueLoggerService.js';

export const NATS_STREAM = {
	DELIVER: 'MISSKEY_DELIVER',
	INBOX: 'MISSKEY_INBOX',
} as const;

export const NATS_SUBJECT = {
	DELIVER: 'misskey.deliver.job',
	INBOX: 'misskey.inbox.job',
} as const;

@Injectable()
export class NatsRelayService implements OnApplicationShutdown {
	private logger: Logger;
	private connection: NatsConnection | null = null;
	private js: JetStreamClient | null = null;
	private jsm: JetStreamManager | null = null;
	private enabled = false;

	constructor(
		@Inject(DI.config)
		private config: Config,

		private queueLoggerService: QueueLoggerService,
	) {
		this.logger = this.queueLoggerService.logger.createSubLogger('nats');
		this.enabled = !!this.config.nats;
	}

	public get isEnabled(): boolean {
		return this.enabled;
	}

	@bindThis
	public async init(): Promise<void> {
		if (!this.config.nats) return;

		this.connection = await connect({
			servers: this.config.nats.servers,
			name: `misskey-${this.config.host}`,
		});

		this.jsm = await this.connection.jetstreamManager();
		this.js = this.connection.jetstream();

		await this.ensureStream(NATS_STREAM.DELIVER, [NATS_SUBJECT.DELIVER]);
		await this.ensureStream(NATS_STREAM.INBOX, [NATS_SUBJECT.INBOX]);

		this.logger.succ('NATS JetStream relay initialized');
	}

	@bindThis
	private async ensureStream(name: string, subjects: string[]): Promise<void> {
		if (!this.jsm) throw new Error('NATS JetStream manager not initialized');

		try {
			await this.jsm.streams.info(name);
			this.logger.debug(`Stream ${name} already exists`);
		} catch {
			await this.jsm.streams.add({
				name,
				subjects,
				// Memory storage: no disk I/O per publish/fetch.
				// Redis+BullMQ is the durable store; NATS is a fast relay bus only.
				storage: StorageType.Memory,
				// Purge messages once all consumers have acked them, keeping the
				// stream compact regardless of how many jobs are in flight.
				retention: RetentionPolicy.Interest,
				max_bytes: -1,
				max_age: 7 * 24 * 60 * 60 * 1_000_000_000, // 7 days in nanoseconds
				discard: DiscardPolicy.Old,
				duplicate_window: 60 * 1_000_000_000, // 1 minute dedup window
			});
			this.logger.succ(`Stream ${name} created`);
		}
	}

	@bindThis
	public async publish(subject: string, data: unknown, msgId?: string): Promise<void> {
		if (!this.js) throw new Error('NATS JetStream not initialized');

		const payload = new TextEncoder().encode(JSON.stringify(data));
		const opts: { msgID?: string } = {};
		if (msgId) opts.msgID = msgId;

		await this.js.publish(subject, payload, opts);
	}

	public getJetStreamClient(): JetStreamClient | null {
		return this.js;
	}

	public getJetStreamManager(): JetStreamManager | null {
		return this.jsm;
	}

	public getConnection(): NatsConnection | null {
		return this.connection;
	}

	@bindThis
	public async close(): Promise<void> {
		if (this.connection) {
			await this.connection.drain();
			await this.connection.close();
			this.connection = null;
			this.js = null;
			this.jsm = null;
			this.logger.succ('NATS connection closed');
		}
	}

	@bindThis
	async onApplicationShutdown(): Promise<void> {
		await this.close();
	}
}
