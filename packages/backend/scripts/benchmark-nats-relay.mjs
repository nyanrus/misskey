/*
 * SPDX-FileCopyrightText: syuilo and misskey-project
 * SPDX-License-Identifier: AGPL-3.0-only
 */

/**
 * Benchmark: Misskey queue throughput & BullMQ ACTIVE list size.
 *
 * Starts a real Misskey backend server, pushes deliver jobs through the
 * BullMQ deliver queue at varying scales, and measures:
 *   - Throughput (jobs/sec)
 *   - Peak ACTIVE list size (the O(n) LREM bottleneck indicator)
 *   - Avg CPU usage (%) during each run
 *   - Avg / Peak RSS memory (MB)
 *
 * Runs benchmarks for each job count in BENCHMARK_JOB_COUNTS, restarting
 * the server between runs for clean measurements.
 *
 * Usage: node scripts/benchmark-nats-relay.mjs
 *
 * Environment variables:
 *   BENCHMARK_JOB_COUNTS - comma-separated job counts (default: 1000,5000,10000,50000)
 *
 * Outputs JSON to stdout (like measure-memory.mjs).
 */

import { fork } from 'node:child_process';
import { setTimeout } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join, resolve } from 'node:path';
import { platform } from 'node:os';
import * as fs from 'node:fs/promises';
import { Queue } from 'bullmq';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const JOB_COUNTS = (process.env.BENCHMARK_JOB_COUNTS ?? '1000,5000,10000,50000')
	.split(',').map(s => parseInt(s.trim()));
const STARTUP_TIMEOUT = 120_000;
const BENCHMARK_TIMEOUT = 300_000;
const SETTLE_TIME = 3_000;
const SAMPLE_INTERVAL_MS = 200;

// -------------------------------------------------------------------
// Process stats sampling (CPU ticks + RSS) via /proc on Linux, ps on macOS
// -------------------------------------------------------------------

async function sampleProcessStats(pid) {
	const now = Date.now();
	if (platform() === 'linux') {
		try {
			const stat = await fs.readFile(`/proc/${pid}/stat`, 'utf-8');
			const fields = stat.split(' ');
			// field 14 = utime, field 15 = stime (clock ticks)
			const cpuTicks = parseInt(fields[13]) + parseInt(fields[14]);

			const status = await fs.readFile(`/proc/${pid}/status`, 'utf-8');
			const rssMatch = status.match(/VmRSS:\s+(\d+)\s+kB/);
			const rssKb = rssMatch ? parseInt(rssMatch[1]) : 0;
			return { cpuTicks, rssKb, wallMs: now };
		} catch {
			return null;
		}
	} else {
		// macOS / other: fall back to ps
		try {
			const { execSync } = await import('node:child_process');
			const out = execSync(`ps -o rss=,cputime= -p ${pid}`, { encoding: 'utf-8' }).trim();
			const [rssStr, timeStr] = out.split(/\s+/);
			const rssKb = parseInt(rssStr) || 0;
			// cputime format is H:MM:SS or MM:SS — convert to centiseconds as proxy ticks
			const parts = (timeStr ?? '0:00:00').split(':').map(Number);
			const totalSec = parts.length === 3
				? parts[0] * 3600 + parts[1] * 60 + parts[2]
				: parts[0] * 60 + parts[1];
			return { cpuTicks: Math.round(totalSec * 100), rssKb, wallMs: now };
		} catch {
			return null;
		}
	}
}

/**
 * Compute avg CPU % from first/last tick samples and avg/peak RSS from all samples.
 * Linux clock ticks = sysconf(_SC_CLK_TCK), typically 100.
 */
function computeStats(samples) {
	const CLK_TCK = 100;
	const validSamples = samples.filter(Boolean);
	if (validSamples.length < 2) {
		return { avgCpuPercent: 0, avgRssMb: 0, peakRssMb: 0 };
	}

	const first = validSamples[0];
	const last = validSamples[validSamples.length - 1];
	const cpuDelta = last.cpuTicks - first.cpuTicks;
	const wallDeltaSec = (last.wallMs - first.wallMs) / 1000;
	const avgCpuPercent = wallDeltaSec > 0
		? Math.round((cpuDelta / CLK_TCK / wallDeltaSec) * 100 * 100) / 100
		: 0;

	const rssValues = validSamples.map(s => s.rssKb);
	const avgRssMb = Math.round(rssValues.reduce((a, b) => a + b, 0) / rssValues.length / 1024 * 100) / 100;
	const peakRssMb = Math.round(Math.max(...rssValues) / 1024 * 100) / 100;

	return { avgCpuPercent, avgRssMb, peakRssMb };
}

// -------------------------------------------------------------------
// Config & Queue helpers
// -------------------------------------------------------------------

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

// -------------------------------------------------------------------
// Server lifecycle
// -------------------------------------------------------------------

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

async function stopServer(serverProcess) {
	serverProcess.kill('SIGTERM');
	let exited = false;
	await new Promise((r) => {
		serverProcess.on('exit', () => { exited = true; r(); });
		setTimeout(10_000).then(() => {
			if (!exited) serverProcess.kill('SIGKILL');
			r();
		});
	});
}

// -------------------------------------------------------------------
// Single benchmark run
// -------------------------------------------------------------------

async function runBenchmark(config, jobCount, serverPid) {
	const mode = config.nats ? 'nats-relay' : 'bullmq-only';
	process.stderr.write(`\n--- ${mode} | ${jobCount} jobs ---\n`);

	const deliverQueue = new Queue('deliver', buildQueueOptions(config, 'deliver'));
	await setTimeout(SETTLE_TIME);

	const jobs = Array.from({ length: jobCount }, (_, i) => ({
		name: `bench-${i}`,
		data: {
			user: { id: '0000000000' },
			content: JSON.stringify({
				'@context': 'https://www.w3.org/ns/activitystreams',
				type: 'Create',
				id: `https://bench.example.com/activities/${i}`,
			}),
			digest: 'sha-256=benchmark',
			to: 'https://bench.example.com/inbox',
			isSharedInbox: false,
		},
		opts: {
			attempts: 1,
			removeOnComplete: true,
			removeOnFail: true,
		},
	}));

	// Start monitoring ACTIVE list + process stats
	let maxActive = 0;
	const procSamples = [];
	const monitorInterval = setInterval(async () => {
		try {
			const counts = await deliverQueue.getJobCounts('active', 'waiting');
			if (counts.active > maxActive) maxActive = counts.active;
		} catch { /* ignore */ }
		const sample = await sampleProcessStats(serverPid);
		if (sample) procSamples.push(sample);
	}, SAMPLE_INTERVAL_MS);

	// Push all jobs
	const startTime = Date.now();
	await deliverQueue.addBulk(jobs);
	const enqueueTime = Date.now() - startTime;
	process.stderr.write(`  Enqueued in ${enqueueTime}ms\n`);

	// Wait for drain
	while (true) {
		await setTimeout(200);
		const counts = await deliverQueue.getJobCounts('active', 'waiting');
		if (counts.active + counts.waiting === 0) break;
		if (Date.now() - startTime > BENCHMARK_TIMEOUT) {
			process.stderr.write('  ⚠ timeout\n');
			break;
		}
	}

	const totalTime = Date.now() - startTime;
	clearInterval(monitorInterval);

	const finalCounts = await deliverQueue.getJobCounts('active', 'waiting', 'completed', 'failed', 'delayed');
	await deliverQueue.close();

	const { avgCpuPercent, avgRssMb, peakRssMb } = computeStats(procSamples);

	const result = {
		mode,
		jobCount,
		enqueueTimeMs: enqueueTime,
		totalTimeMs: totalTime,
		throughputJobsPerSec: Math.round(jobCount / (totalTime / 1000)),
		maxActiveListSize: maxActive,
		avgCpuPercent,
		avgRssMb,
		peakRssMb,
		sampleCount: procSamples.length,
		finalCounts,
	};

	process.stderr.write(`  Throughput: ${result.throughputJobsPerSec} jobs/sec\n`);
	process.stderr.write(`  Max ACTIVE: ${maxActive}\n`);
	process.stderr.write(`  Avg CPU:    ${avgCpuPercent}%\n`);
	process.stderr.write(`  Avg RSS:    ${avgRssMb} MB  |  Peak RSS: ${peakRssMb} MB\n`);

	return result;
}

// -------------------------------------------------------------------
// Main: loop over job counts, restart server each time
// -------------------------------------------------------------------

async function main() {
	const config = await readConfig();
	const mode = config.nats ? 'nats-relay' : 'bullmq-only';
	process.stderr.write(`Benchmark mode: ${mode}\n`);
	process.stderr.write(`Job counts: ${JOB_COUNTS.join(', ')}\n`);

	const results = [];

	for (const jobCount of JOB_COUNTS) {
		const { serverProcess, startupTime } = await startServer();
		try {
			const result = await runBenchmark(config, jobCount, serverProcess.pid);
			results.push({ ...result, startupTimeMs: startupTime });
		} finally {
			await stopServer(serverProcess);
		}
	}

	console.log(JSON.stringify({
		timestamp: new Date().toISOString(),
		mode,
		runs: results,
	}, null, 2));
}

main().catch((err) => {
	console.error(JSON.stringify({ error: err.message, timestamp: new Date().toISOString() }));
	process.exit(1);
});

