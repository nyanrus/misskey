/*
 * SPDX-FileCopyrightText: syuilo and misskey-project
 * SPDX-License-Identifier: AGPL-3.0-only
 */

/**
 * Benchmark: Misskey queue throughput & BullMQ ACTIVE list size.
 *
 * This script starts a real Misskey backend server, pushes deliver jobs
 * through the BullMQ deliver queue, and measures:
 *   - Throughput (jobs/sec)
 *   - Peak ACTIVE list size (the O(n) LREM bottleneck indicator)
 *   - Total processing time
 *
 * When NATS JetStream is configured in the Misskey config, the relay
 * architecture is active and the ACTIVE list should stay near 0.
 * When NATS is not configured, BullMQ processes jobs directly.
 *
 * Usage: node scripts/benchmark-nats-relay.mjs
 *
 * Environment variables:
 *   BENCHMARK_JOB_COUNT  - number of jobs to enqueue (default: 500)
 *
 * Outputs JSON to stdout (like measure-memory.mjs).
 */

import { fork } from 'node:child_process';
import { setTimeout } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join, resolve } from 'node:path';
import * as fs from 'node:fs/promises';
import { Queue } from 'bullmq';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const JOB_COUNT = parseInt(process.env.BENCHMARK_JOB_COUNT ?? '500');
const STARTUP_TIMEOUT = 120_000;
const BENCHMARK_TIMEOUT = 300_000;
const SETTLE_TIME = 3_000;

/**
 * Read compiled Misskey config to get Redis connection and queue prefix.
 */
async function readConfig() {
	const builtDir = resolve(__dirname, '../../../built');
	const testPath = resolve(builtDir, '._config_.json');
	const normalPath = resolve(builtDir, '.config.json');

	let configPath;
	try {
		await fs.access(testPath);
		configPath = testPath;
	} catch {
		configPath = normalPath;
	}

	return JSON.parse(await fs.readFile(configPath, 'utf-8'));
}

/**
 * Build BullMQ QueueOptions matching Misskey's queue prefix scheme.
 * Must match packages/backend/src/queue/const.ts baseQueueOptions().
 */
function buildQueueOptions(config, queueName) {
	const redis = config.redisForJobQueue ?? config.redis;
	const urlHost = new URL(config.url).host;
	const prefix = redis.prefix ?? urlHost;

	return {
		connection: {
			host: redis.host,
			port: redis.port,
			password: redis.pass || undefined,
			db: redis.db ?? 0,
			family: redis.family ?? 0,
		},
		prefix: prefix ? `${prefix}:queue:${queueName}` : `queue:${queueName}`,
	};
}

/**
 * Fork the Misskey backend server and wait until it signals readiness.
 */
async function startServer() {
	const serverProcess = fork(join(__dirname, '../built/boot/entry.js'), [], {
		cwd: join(__dirname, '..'),
		env: {
			...process.env,
			NODE_ENV: 'production',
			MK_DISABLE_CLUSTERING: '1',
		},
		stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
	});

	let ready = false;
	serverProcess.on('message', (msg) => { if (msg === 'ok') ready = true; });
	serverProcess.stdout?.on('data', (d) => process.stderr.write(`[server] ${d}`));
	serverProcess.stderr?.on('data', (d) => process.stderr.write(`[server] ${d}`));
	serverProcess.on('error', (err) => process.stderr.write(`[server error] ${err}\n`));

	const t0 = Date.now();
	while (!ready) {
		if (Date.now() - t0 > STARTUP_TIMEOUT) {
			serverProcess.kill('SIGTERM');
			throw new Error('Server startup timeout');
		}
		await setTimeout(100);
	}

	const startupTime = Date.now() - t0;
	process.stderr.write(`Server ready in ${startupTime}ms\n`);

	return { serverProcess, startupTime };
}

/**
 * Stop the server process gracefully.
 */
async function stopServer(serverProcess) {
	serverProcess.kill('SIGTERM');
	let exited = false;
	await new Promise((resolve) => {
		serverProcess.on('exit', () => { exited = true; resolve(); });
		setTimeout(10_000).then(() => {
			if (!exited) serverProcess.kill('SIGKILL');
			resolve();
		});
	});
}

/**
 * Run the benchmark: push jobs and monitor queue stats.
 */
async function runBenchmark(config) {
	const mode = config.nats ? 'nats-relay' : 'bullmq-only';
	process.stderr.write(`\nBenchmark mode: ${mode}, jobs: ${JOB_COUNT}\n`);

	// Connect to the same deliver queue that Misskey's worker is consuming
	const deliverQueue = new Queue('deliver', buildQueueOptions(config, 'deliver'));

	// Wait for workers to settle
	await setTimeout(SETTLE_TIME);

	// Prepare deliver jobs with realistic shape but fake user.
	// The deliver processor will attempt signedPost with a non-existent user,
	// fail fast, and the job goes ACTIVE → FAILED, exercising BullMQ LREM.
	// In NATS relay mode, the relay publishes to NATS instantly,
	// and the job goes ACTIVE → COMPLETED (~0 ACTIVE list).
	const jobs = Array.from({ length: JOB_COUNT }, (_, i) => ({
		name: `bench-${i}`,
		data: {
			user: { id: '0000000000' },
			content: JSON.stringify({
				'@context': 'https://www.w3.org/ns/activitystreams',
				type: 'Create',
				id: `https://bench.example.com/activities/${i}`,
			}),
			digest: 'sha-256=benchmark',
			to: `https://bench.example.com/inbox`,
			isSharedInbox: false,
		},
		opts: {
			attempts: 1,
			removeOnComplete: true,
			removeOnFail: true,
		},
	}));

	// Start monitoring ACTIVE list
	let maxActive = 0;
	const activeSamples = [];
	const monitorInterval = setInterval(async () => {
		try {
			const counts = await deliverQueue.getJobCounts('active', 'waiting', 'completed', 'failed');
			if (counts.active > maxActive) maxActive = counts.active;
			activeSamples.push({ t: Date.now(), a: counts.active, w: counts.waiting });
		} catch { /* ignore */ }
	}, 50);

	// Push all jobs
	const startTime = Date.now();
	await deliverQueue.addBulk(jobs);
	const enqueueTime = Date.now() - startTime;
	process.stderr.write(`Enqueued ${JOB_COUNT} jobs in ${enqueueTime}ms\n`);

	// Wait for all jobs to leave ACTIVE+WAITING
	while (true) {
		await setTimeout(200);
		const counts = await deliverQueue.getJobCounts('active', 'waiting');
		if (counts.active + counts.waiting === 0) break;
		if (Date.now() - startTime > BENCHMARK_TIMEOUT) {
			process.stderr.write('Benchmark timeout waiting for jobs to complete\n');
			break;
		}
	}

	const totalTime = Date.now() - startTime;
	clearInterval(monitorInterval);

	// Final counts
	const finalCounts = await deliverQueue.getJobCounts('active', 'waiting', 'completed', 'failed', 'delayed');

	await deliverQueue.close();

	const result = {
		mode,
		jobCount: JOB_COUNT,
		enqueueTimeMs: enqueueTime,
		totalTimeMs: totalTime,
		throughputJobsPerSec: Math.round(JOB_COUNT / (totalTime / 1000)),
		maxActiveListSize: maxActive,
		activeSampleCount: activeSamples.length,
		finalCounts,
	};

	process.stderr.write(`  Throughput:     ${result.throughputJobsPerSec} jobs/sec\n`);
	process.stderr.write(`  Total time:     ${totalTime}ms\n`);
	process.stderr.write(`  Max ACTIVE:     ${maxActive}\n`);
	process.stderr.write(`  Final counts:   ${JSON.stringify(finalCounts)}\n`);

	return result;
}

async function main() {
	const config = await readConfig();

	const { serverProcess, startupTime } = await startServer();

	try {
		const benchmarkResult = await runBenchmark(config);

		// Output JSON to stdout
		console.log(JSON.stringify({
			timestamp: new Date().toISOString(),
			startupTimeMs: startupTime,
			...benchmarkResult,
		}, null, 2));
	} finally {
		await stopServer(serverProcess);
	}
}

main().catch((err) => {
	console.error(JSON.stringify({ error: err.message, timestamp: new Date().toISOString() }));
	process.exit(1);
});

