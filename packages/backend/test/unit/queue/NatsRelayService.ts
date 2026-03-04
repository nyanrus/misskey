/*
 * SPDX-FileCopyrightText: syuilo and misskey-project
 * SPDX-License-Identifier: AGPL-3.0-only
 */

import { describe, expect, jest, test, beforeEach } from '@jest/globals';
import { NatsRelayService, NATS_STREAM, NATS_SUBJECT } from '@/queue/NatsRelayService.js';

/* eslint-disable @typescript-eslint/no-explicit-any */

describe('NatsRelayService', () => {
	describe('isEnabled', () => {
		test('should be disabled when nats config is not set', () => {
			const service = createService({ nats: undefined });
			expect(service.isEnabled).toBe(false);
		});

		test('should be enabled when nats config is set', () => {
			const service = createService({ nats: { servers: ['nats://localhost:4222'] } });
			expect(service.isEnabled).toBe(true);
		});
	});

	describe('init', () => {
		test('should not connect when nats config is not set', async () => {
			const service = createService({ nats: undefined });
			await service.init();
			expect(service.getConnection()).toBeNull();
			expect(service.getJetStreamClient()).toBeNull();
		});
	});

	describe('publish', () => {
		test('should throw when not initialized', async () => {
			const service = createService({ nats: { servers: ['nats://localhost:4222'] } });
			await expect(service.publish(NATS_SUBJECT.DELIVER, { test: true }))
				.rejects.toThrow('NATS JetStream not initialized');
		});
	});

	describe('constants', () => {
		test('stream names should be defined', () => {
			expect(NATS_STREAM.DELIVER).toBe('MISSKEY_DELIVER');
			expect(NATS_STREAM.INBOX).toBe('MISSKEY_INBOX');
		});

		test('subject names should be defined', () => {
			expect(NATS_SUBJECT.DELIVER).toBe('misskey.deliver.job');
			expect(NATS_SUBJECT.INBOX).toBe('misskey.inbox.job');
		});
	});

	describe('close', () => {
		test('should be safe to call when not initialized', async () => {
			const service = createService({ nats: undefined });
			await expect(service.close()).resolves.toBeUndefined();
		});
	});
});

function createService(configOverrides: { nats?: { servers: string[] } }): NatsRelayService {
	const mockConfig = {
		nats: configOverrides.nats,
	} as any;

	const mockLoggerService = {
		logger: {
			createSubLogger: () => ({
				debug: () => {},
				info: () => {},
				succ: () => {},
				warn: () => {},
				error: () => {},
				createSubLogger: () => ({
					debug: () => {},
					info: () => {},
					succ: () => {},
					warn: () => {},
					error: () => {},
				}),
			}),
		},
	} as any;

	return new NatsRelayService(mockConfig, mockLoggerService);
}
