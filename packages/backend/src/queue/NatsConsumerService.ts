/*
 * SPDX-FileCopyrightText: syuilo and misskey-project
 * SPDX-License-Identifier: AGPL-3.0-only
 */

import { Inject, Injectable, OnApplicationShutdown } from '@nestjs/common';
import * as Bull from 'bullmq';
import { AckPolicy, DeliverPolicy, type ConsumerMessages, type JetStreamManager } from 'nats';
import type { Config } from '@/config.js';
import { DI } from '@/di-symbols.js';
import type Logger from '@/logger.js';
import { bindThis } from '@/decorators.js';
import { DeliverProcessorService } from './processors/DeliverProcessorService.js';
import { InboxProcessorService } from './processors/InboxProcessorService.js';
import { QueueLoggerService } from './QueueLoggerService.js';
import { NatsRelayService, NATS_STREAM, NATS_SUBJECT } from './NatsRelayService.js';
import type { DeliverJobData, InboxJobData } from './types.js';

@Injectable()
export class NatsConsumerService implements OnApplicationShutdown {
	private logger: Logger;
	private deliverConsumer: ConsumerMessages | null = null;
	private inboxConsumer: ConsumerMessages | null = null;
	private running = false;

	constructor(
		@Inject(DI.config)
		private config: Config,

		private natsRelayService: NatsRelayService,
		private deliverProcessorService: DeliverProcessorService,
		private inboxProcessorService: InboxProcessorService,
		private queueLoggerService: QueueLoggerService,
	) {
		this.logger = this.queueLoggerService.logger.createSubLogger('nats-consumer');
	}

	@bindThis
	public async start(): Promise<void> {
		if (!this.natsRelayService.isEnabled) return;

		const js = this.natsRelayService.getJetStreamClient();
		const jsm = this.natsRelayService.getJetStreamManager();
		if (!js || !jsm) throw new Error('NATS JetStream not initialized');

		await this.ensureConsumer(jsm, NATS_STREAM.DELIVER, 'misskey-deliver-worker', NATS_SUBJECT.DELIVER);
		await this.ensureConsumer(jsm, NATS_STREAM.INBOX, 'misskey-inbox-worker', NATS_SUBJECT.INBOX);

		const deliverMaxAckPending = this.config.deliverJobConcurrency ?? 128;
		const inboxMaxAckPending = this.config.inboxJobConcurrency ?? 16;

		this.running = true;

		const deliverConsumer = await js.consumers.get(NATS_STREAM.DELIVER, 'misskey-deliver-worker');
		this.deliverConsumer = await deliverConsumer.consume({ max_messages: deliverMaxAckPending });
		this.processMessages(this.deliverConsumer, 'deliver', async (data: DeliverJobData) => {
			const fakeJob = { data } as Bull.Job<DeliverJobData>;
			return this.deliverProcessorService.process(fakeJob);
		});

		const inboxConsumer = await js.consumers.get(NATS_STREAM.INBOX, 'misskey-inbox-worker');
		this.inboxConsumer = await inboxConsumer.consume({ max_messages: inboxMaxAckPending });
		this.processMessages(this.inboxConsumer, 'inbox', async (data: InboxJobData) => {
			const fakeJob = { data } as Bull.Job<InboxJobData>;
			return this.inboxProcessorService.process(fakeJob);
		});

		this.logger.succ('NATS consumers started');
	}

	@bindThis
	private async ensureConsumer(jsm: JetStreamManager, stream: string, name: string, subject: string): Promise<void> {
		try {
			await jsm.consumers.info(stream, name);
		} catch {
			await jsm.consumers.add(stream, {
				durable_name: name,
				ack_policy: AckPolicy.Explicit,
				deliver_policy: DeliverPolicy.All,
				filter_subject: subject,
				max_ack_pending: stream === NATS_STREAM.DELIVER
					? (this.config.deliverJobConcurrency ?? 128)
					: (this.config.inboxJobConcurrency ?? 16),
				ack_wait: 10 * 60 * 1_000_000_000, // 10 minutes in nanoseconds
			});
		}
	}

	@bindThis
	private async processMessages<T>(
		consumer: ConsumerMessages,
		queueName: string,
		handler: (data: T) => Promise<string>,
	): Promise<void> {
		const logger = this.logger.createSubLogger(queueName);

		(async () => {
			for await (const msg of consumer) {
				if (!this.running) break;

				try {
					const data = JSON.parse(new TextDecoder().decode(msg.data)) as T;
					logger.debug(`processing seq=${msg.seq}`);
					const result = await handler(data);
					msg.ack();
					logger.debug(`completed(${result}) seq=${msg.seq}`);
				} catch (err: unknown) {
					const error = err as Error;
					if (error instanceof Bull.UnrecoverableError || error.name === 'AbortError') {
						// Non-retryable: ack to discard
						msg.ack();
						logger.error(`unrecoverable(${error.name}: ${error.message}) seq=${msg.seq}`);
					} else {
						// Retryable: nak with delay for backpressure
						msg.nak(60_000); // retry after 60s
						logger.error(`failed(${error.name}: ${error.message}) seq=${msg.seq}, will retry`);
					}
				}
			}
		})();
	}

	@bindThis
	public async stop(): Promise<void> {
		this.running = false;
		if (this.deliverConsumer) {
			this.deliverConsumer.stop();
			this.deliverConsumer = null;
		}
		if (this.inboxConsumer) {
			this.inboxConsumer.stop();
			this.inboxConsumer = null;
		}
		this.logger.succ('NATS consumers stopped');
	}

	@bindThis
	async onApplicationShutdown(): Promise<void> {
		await this.stop();
	}
}
