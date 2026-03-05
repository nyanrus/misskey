/*
 * SPDX-FileCopyrightText: syuilo and misskey-project
 * SPDX-License-Identifier: AGPL-3.0-only
 */

import { Inject, Injectable, OnApplicationShutdown } from '@nestjs/common';
import * as Bull from 'bullmq';
import { connect, AckPolicy, DeliverPolicy, DiscardPolicy, RetentionPolicy, StorageType, type NatsConnection, type ConsumerMessages, type JetStreamManager } from 'nats';
import type { Config } from '@/config.js';
import { DI } from '@/di-symbols.js';
import type Logger from '@/logger.js';
import { bindThis } from '@/decorators.js';
import { NATS_DELIVER_DIRECT_STREAM, NATS_DELIVER_DIRECT_SUBJECT } from '@/core/QueueService.js';
import type { DeliverQueue } from '@/core/QueueModule.js';
import { DeliverProcessorService } from './processors/DeliverProcessorService.js';
import { InboxProcessorService } from './processors/InboxProcessorService.js';
import { QueueLoggerService } from './QueueLoggerService.js';
import { NatsRelayService, NATS_STREAM, NATS_SUBJECT, type NatsEnvelope } from './NatsRelayService.js';
import type { DeliverJobData, InboxJobData } from './types.js';

/** Exponential backoff for HTTP-related transient failures (in milliseconds). */
function httpRelatedBackoff(attempt: number): number {
	// 5s, 10s, 30s, 60s, 120s, 300s, 600s (capped)
	const delays = [5_000, 10_000, 30_000, 60_000, 120_000, 300_000, 600_000];
	return delays[Math.min(attempt, delays.length - 1)];
}

@Injectable()
export class NatsConsumerService implements OnApplicationShutdown {
	private logger: Logger;
	private connection: NatsConnection | null = null;
	private deliverConsumer: ConsumerMessages | null = null;
	private inboxConsumer: ConsumerMessages | null = null;
	private running = false;
	private readonly decoder = new TextDecoder();

	constructor(
		@Inject(DI.config)
		private config: Config,

		@Inject('queue:deliver')
		private deliverQueue: DeliverQueue,

		private natsRelayService: NatsRelayService,
		private deliverProcessorService: DeliverProcessorService,
		private inboxProcessorService: InboxProcessorService,
		private queueLoggerService: QueueLoggerService,
	) {
		this.logger = this.queueLoggerService.logger.createSubLogger('nats-consumer');
	}

	@bindThis
	public async start(): Promise<void> {
		if (!this.config.nats) return;

		// Dedicated connection for consuming — keeps publish and consume
		// traffic on separate TCP sockets so they don't starve each other
		// under a 50k publish flood.
		this.connection = await connect({
			servers: this.config.nats.servers,
			name: `misskey-${this.config.host}-consumer`,
		});
		const js = this.connection.jetstream();
		const jsm = await this.connection.jetstreamManager();

		const processingConcurrency = this.config.deliverJobConcurrency ?? 128;
		const natsWindow = Math.min(
			Math.max(processingConcurrency * 4, 512),
			4096,
		);
		const deliverMaxAckPending = this.config.natsDeliverConcurrency ?? natsWindow;
		const inboxMaxAckPending = this.config.natsInboxConcurrency ?? this.config.inboxJobConcurrency ?? 16;

		this.running = true;

		// Direct mode: ensure the deliver direct stream exists before subscribing.
		// QueueService also creates it lazily, but the consumer may start first.
		await this.ensureStream(jsm, NATS_DELIVER_DIRECT_STREAM, [NATS_DELIVER_DIRECT_SUBJECT]);

		// Direct mode: deliver consumer reads from the direct stream (QueueService publishes raw DeliverJobData)
		const maxDeliverAttempts = this.config.deliverJobMaxAttempts ?? 12;
		await this.ensureConsumer(jsm, NATS_DELIVER_DIRECT_STREAM, 'misskey-deliver-direct', NATS_DELIVER_DIRECT_SUBJECT, deliverMaxAckPending, maxDeliverAttempts);
		const deliverConsumer = await js.consumers.get(NATS_DELIVER_DIRECT_STREAM, 'misskey-deliver-direct');
		this.deliverConsumer = await deliverConsumer.consume({ max_messages: deliverMaxAckPending });
		this.processDirectMessages(this.deliverConsumer, 'deliver', async (data: DeliverJobData) => {
			const fakeJob = { data } as Bull.Job<DeliverJobData>;
			return this.deliverProcessorService.process(fakeJob);
		});

		// Relay mode: inbox consumer reads from the relay stream (NatsRelayService publishes NatsEnvelope)
		if (this.natsRelayService.isEnabled) {
			await this.ensureConsumer(jsm, NATS_STREAM.INBOX, 'misskey-inbox-worker', NATS_SUBJECT.INBOX, inboxMaxAckPending);
			const inboxConsumer = await js.consumers.get(NATS_STREAM.INBOX, 'misskey-inbox-worker');
			this.inboxConsumer = await inboxConsumer.consume({ max_messages: inboxMaxAckPending });
			this.processRelayMessages(this.inboxConsumer, NATS_SUBJECT.INBOX, 'inbox', async (data: InboxJobData) => {
				const fakeJob = { data } as Bull.Job<InboxJobData>;
				return this.inboxProcessorService.process(fakeJob);
			});
		}

		this.logger.succ(`NATS consumers started (deliver direct max_ack_pending=${deliverMaxAckPending}, inbox relay max_ack_pending=${inboxMaxAckPending})`);
	}

	@bindThis
	private async ensureConsumer(jsm: JetStreamManager, stream: string, name: string, subject: string, maxAckPending: number, maxDeliver?: number): Promise<void> {
		const config = {
			max_ack_pending: maxAckPending,
			ack_wait: 10 * 60 * 1_000_000_000, // 10 minutes in nanoseconds
			...(maxDeliver != null ? { max_deliver: maxDeliver } : {}),
		};
		try {
			await jsm.consumers.info(stream, name);
			await jsm.consumers.update(stream, name, config);
		} catch {
			await jsm.consumers.add(stream, {
				durable_name: name,
				ack_policy: AckPolicy.Explicit,
				deliver_policy: DeliverPolicy.All,
				filter_subject: subject,
				...config,
			});
		}
	}

	@bindThis
	private async ensureStream(jsm: JetStreamManager, name: string, subjects: string[]): Promise<void> {
		try {
			await jsm.streams.info(name);
		} catch {
			await jsm.streams.add({
				name,
				subjects,
				retention: RetentionPolicy.Workqueue,
				storage: StorageType.File,
				discard: DiscardPolicy.New,
				max_msgs: 1_000_000,
			});
		}
	}

	/**
	 * Direct mode: NATS owns retry semantics.
	 * - On success: msg.ack()
	 * - On transient failure: msg.nak(backoffDelay)
	 * - On permanent failure: msg.term() + push to BullMQ as nats-failed
	 */
	@bindThis
	private async processDirectMessages<T>(
		consumer: ConsumerMessages,
		queueName: string,
		handler: (data: T) => Promise<string>,
	): Promise<void> {
		const logger = this.logger.createSubLogger(queueName);
		const maxAttempts = this.config.deliverJobMaxAttempts ?? 12;

		(async () => {
			for await (const msg of consumer) {
				if (!this.running) break;

				let data: T;
				try {
					data = JSON.parse(this.decoder.decode(msg.data)) as T;
				} catch {
					msg.term();
					continue;
				}

				const redeliveryCount = msg.info.redeliveryCount;
				logger.debug(`processing seq=${msg.seq} attempt=${redeliveryCount}`);

				handler(data)
					.then((result) => {
						msg.ack();
						logger.debug(`completed(${result}) seq=${msg.seq}`);
					})
					.catch((err: unknown) => {
						const error = err as Error;
						const isPermanent = error instanceof Bull.UnrecoverableError || error.name === 'AbortError';

						if (isPermanent || redeliveryCount >= maxAttempts) {
							// Permanent failure — terminate in NATS, push to BullMQ for admin visibility
							msg.term();
							logger.error(`permanent failure(${error.name}: ${error.message}) seq=${msg.seq}`);
							this.pushFailedToBullMQ(data, error.message).catch(() => {});
						} else {
							// Transient failure — NAK with exponential backoff
							const delay = httpRelatedBackoff(redeliveryCount);
							msg.nak(delay);
							logger.warn(`transient failure(${error.name}: ${error.message}) seq=${msg.seq}, retry in ${delay}ms`);
						}
					});
			}
		})();
	}

	/**
	 * Relay mode: BullMQ owns retry semantics.
	 * Messages are always acked; relay promises are resolved/rejected.
	 */
	@bindThis
	private async processRelayMessages<T>(
		consumer: ConsumerMessages,
		subject: string,
		queueName: string,
		handler: (data: T) => Promise<string>,
	): Promise<void> {
		const logger = this.logger.createSubLogger(queueName);

		(async () => {
			for await (const msg of consumer) {
				if (!this.running) break;

				let jobId: string;
				let data: T;
				try {
					const envelope = JSON.parse(this.decoder.decode(msg.data)) as NatsEnvelope<T>;
					jobId = envelope.jobId;
					data = envelope.data;
				} catch {
					msg.ack();
					continue;
				}

				logger.debug(`processing seq=${msg.seq} jobId=${jobId}`);
				handler(data)
					.then((result) => {
						msg.ack();
						this.natsRelayService.resolveJob(subject, jobId, result);
						logger.debug(`completed(${result}) seq=${msg.seq}`);
					})
					.catch((err: unknown) => {
						const error = err as Error;
						msg.ack();
						this.natsRelayService.rejectJob(subject, jobId, error);
						if (error instanceof Bull.UnrecoverableError || error.name === 'AbortError') {
							logger.error(`unrecoverable(${error.name}: ${error.message}) seq=${msg.seq}`);
						} else {
							logger.error(`failed(${error.name}: ${error.message}) seq=${msg.seq}, BullMQ will retry`);
						}
					});
			}
		})();
	}

	/** Push a permanently failed delivery to BullMQ so admins can see it in the dashboard. */
	@bindThis
	private async pushFailedToBullMQ(data: unknown, reason: string): Promise<void> {
		await this.deliverQueue.add('nats-failed', { ...(data as object), reason } as any, {
			attempts: 1,
			removeOnFail: { age: 3600 * 24 * 7, count: 100 },
		});
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
		if (this.connection) {
			await this.connection.drain();
			await this.connection.close();
			this.connection = null;
		}
		this.logger.succ('NATS consumers stopped');
	}

	@bindThis
	async onApplicationShutdown(): Promise<void> {
		await this.stop();
	}
}
