/*
 * SPDX-FileCopyrightText: syuilo and misskey-project
 * SPDX-License-Identifier: AGPL-3.0-only
 */

/**
 * Benchmark: Misskey note creation → federation deliver throughput.
 *
 * Starts a real Misskey backend server, seeds remote followers (pointing
 * to local mock HTTP servers), then continuously posts notes via the
 * notes/create API. Each note triggers one deliver job per remote follower,
 * exercising the full pipeline:
 *
 *   notes/create API → NoteCreateService → DeliverManagerService
 *     → BullMQ deliver queue (→ NATS relay if enabled)
 *     → HTTP POST to mock inbox servers
 *
 * Measures per target rate:
 *   - Note creation throughput (notes/sec achieved vs target)
 *   - Deliver queue throughput (HTTP inbox hits/sec)
 *   - API latency (p50, p95, p99)
 *   - Server CPU and RSS memory
 *   - Redis CPU usage
 *
 * Usage: node scripts/benchmark-note-throughput.mjs
 *
 * Environment variables:
 *   BENCHMARK_RATES         - comma-separated target note rates in notes/sec
 *                             (default: 1000,5000,10000,50000)
 *   BENCHMARK_DURATION_MS   - sustain time per rate in ms (default: 10000)
 *   BENCHMARK_CONCURRENCY   - max in-flight notes/create requests (default: 200)
 *   BENCHMARK_FOLLOWER_COUNT - number of remote followers to seed (default: 5)
 *                              total deliver jobs/sec = notes/sec × follower count
 *   BENCHMARK_DELAY_MS      - mock inbox response delay in ms (default: 1000)
 */

import { fork, execSync } from 'node:child_process';
import { createServer } from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join, resolve } from 'node:path';
import { platform } from 'node:os';
import { randomBytes } from 'node:crypto';
import * as fs from 'node:fs/promises';
import { Queue } from 'bullmq';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const BENCHMARK_RATES = (process.env.BENCHMARK_RATES ?? '1000,5000,10000,50000')
	.split(',').map(s => parseInt(s.trim()));
const DURATION_MS = parseInt(process.env.BENCHMARK_DURATION_MS ?? '10000');
const CONCURRENCY = parseInt(process.env.BENCHMARK_CONCURRENCY ?? '200');
const FOLLOWER_COUNT = parseInt(process.env.BENCHMARK_FOLLOWER_COUNT ?? '5000');
const MOCK_DELAY_MS = parseInt(process.env.BENCHMARK_DELAY_MS ?? '1000');

const STARTUP_TIMEOUT = 120_000;
const SETTLE_TIME = 3_000;
const SAMPLE_INTERVAL_MS = 200;
const MOCK_PORTS = Array.from({ length: 1000 }, (_, i) => 19199 + i);
const BENCH_USER_FILE = resolve(__dirname, '../../../.bench-user.json');

// -------------------------------------------------------------------
// Minimal aidx ID generator (matches packages/backend/src/misc/id/aidx.ts)
// -------------------------------------------------------------------

const TIME2000 = 946684800000;
const nodeId = randomBytes(2).toString('hex').slice(0, 4);
let aidxCounter = 0;

function genId(t = Date.now()) {
	const time = Math.max(0, t - TIME2000).toString(36).padStart(8, '0').slice(-8);
	const noise = (aidxCounter++).toString(36).padStart(4, '0').slice(-4);
	return time + nodeId + noise;
}

// -------------------------------------------------------------------
// Mock HTTP servers — simulate remote inbox endpoints
// -------------------------------------------------------------------

function startMockServers(baseDelayMs, ports) {
	let totalRequestCount = 0;
	const servers = [];
	return new Promise((resolveP) => {
		let started = 0;
		for (const port of ports) {
			const server = createServer((req, resp) => {
				totalRequestCount++;
				req.resume();
				req.on('end', () => {
					globalThis.setTimeout(() => { resp.writeHead(202); resp.end('accepted'); }, baseDelayMs);
				});
			});
			server.keepAliveTimeout = 60_000;
			servers.push(server);
			server.listen(port, '127.0.0.1', () => {
				if (++started === ports.length) {
					process.stderr.write('Mock servers on ports ' + ports[0] + '-' + ports[ports.length - 1] + '\n');
					resolveP({
						getRequestCount: () => totalRequestCount,
						resetRequestCount: () => { totalRequestCount = 0; },
						close: () => Promise.all(servers.map(s => new Promise(r => s.close(r)))),
					});
				}
			});
		}
	});
}

// -------------------------------------------------------------------
// Process stats (CPU ticks + RSS)
// -------------------------------------------------------------------

async function sampleProcessStats(pid) {
	const now = Date.now();
	if (platform() === 'linux') {
		try {
			const stat = await fs.readFile('/proc/' + pid + '/stat', 'utf-8');
			const fields = stat.split(' ');
			const cpuTicks = parseInt(fields[13]) + parseInt(fields[14]);
			const status = await fs.readFile('/proc/' + pid + '/status', 'utf-8');
			const rssMatch = status.match(/VmRSS:\s+(\d+)\s+kB/);
			return { cpuTicks, rssKb: rssMatch ? parseInt(rssMatch[1]) : 0, wallMs: now };
		} catch { return null; }
	} else {
		try {
			const out = execSync('ps -o rss=,cputime= -p ' + pid, { encoding: 'utf-8' }).trim();
			const [rssStr, timeStr] = out.split(/\s+/);
			const parts = (timeStr ?? '0:00:00').split(':').map(Number);
			const totalSec = parts.length === 3
				? parts[0] * 3600 + parts[1] * 60 + parts[2]
				: parts[0] * 60 + parts[1];
			return { cpuTicks: Math.round(totalSec * 100), rssKb: parseInt(rssStr) || 0, wallMs: now };
		} catch { return null; }
	}
}

function computeProcessStats(samples) {
	const valid = samples.filter(Boolean);
	if (valid.length < 2) return { avgCpuPercent: 0, avgRssMb: 0, peakRssMb: 0 };
	const first = valid[0], last = valid[valid.length - 1];
	const wallSec = (last.wallMs - first.wallMs) / 1000;
	const avgCpuPercent = wallSec > 0
		? Math.round((last.cpuTicks - first.cpuTicks) / 100 / wallSec * 100 * 100) / 100 : 0;
	const rss = valid.map(s => s.rssKb);
	return {
		avgCpuPercent,
		avgRssMb: Math.round(rss.reduce((a, b) => a + b, 0) / rss.length / 1024 * 100) / 100,
		peakRssMb: Math.round(Math.max(...rss) / 1024 * 100) / 100,
	};
}

// -------------------------------------------------------------------
// Redis CPU tracking
// -------------------------------------------------------------------

async function getRedisStats(redisConfig) {
	const { default: Redis } = await import('ioredis');
	const client = new Redis({
		host: redisConfig.host, port: redisConfig.port,
		password: redisConfig.pass || undefined, db: redisConfig.db ?? 0,
	});
	const info = await client.info('cpu');
	await client.quit();
	const cpuSys = parseFloat(info.match(/used_cpu_sys:([\d.]+)/)?.[1] ?? '0');
	const cpuUser = parseFloat(info.match(/used_cpu_user:([\d.]+)/)?.[1] ?? '0');
	return { cpuTotal: Math.round((cpuSys + cpuUser) * 1000) / 1000 };
}

// -------------------------------------------------------------------
// Config helpers
// -------------------------------------------------------------------

async function readConfig() {
	const builtDir = resolve(__dirname, '../../../built');
	const testPath = resolve(builtDir, '._config_.json');
	const normalPath = resolve(builtDir, '.config.json');
	let configPath;
	try { await fs.access(testPath); configPath = testPath; } catch { configPath = normalPath; }
	return { config: JSON.parse(await fs.readFile(configPath, 'utf-8')), configPath };
}

async function patchConfigForBenchmark(configPath) {
	const original = await fs.readFile(configPath, 'utf-8');
	const config = JSON.parse(original);
	if (!config.allowedPrivateNetworks) config.allowedPrivateNetworks = [];
	if (!config.allowedPrivateNetworks.includes('127.0.0.0/8')) {
		config.allowedPrivateNetworks.push('127.0.0.0/8');
	}
	await fs.writeFile(configPath, JSON.stringify(config, null, '\t'));
	return original;
}

async function restoreConfig(configPath, original) {
	await fs.writeFile(configPath, original);
}

function buildQueueOptions(config, queueName) {
	const redis = config.redisForJobQueue ?? config.redis;
	const urlHost = new URL(config.url).host;
	const prefix = redis.prefix ?? urlHost;
	return {
		connection: {
			host: redis.host, port: redis.port,
			password: redis.pass || undefined, db: redis.db ?? 0, family: redis.family ?? 0,
		},
		prefix: prefix ? prefix + ':queue:' + queueName : 'queue:' + queueName,
	};
}

// -------------------------------------------------------------------
// Server lifecycle
// -------------------------------------------------------------------

async function startServer() {
	const serverProcess = fork(join(__dirname, '../built/boot/entry.js'), [], {
		cwd: join(__dirname, '..'),
		env: { ...process.env, NODE_ENV: 'test', MK_DISABLE_CLUSTERING: '1' },
		stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
	});
	let ready = false;
	serverProcess.on('message', (msg) => { if (msg === 'ok') ready = true; });
	serverProcess.stdout?.on('data', (d) => process.stderr.write('[server] ' + d));
	serverProcess.stderr?.on('data', (d) => process.stderr.write('[server] ' + d));
	serverProcess.on('error', (err) => process.stderr.write('[server error] ' + err + '\n'));
	const t0 = Date.now();
	while (!ready) {
		if (Date.now() - t0 > STARTUP_TIMEOUT) {
			try { process.kill(serverProcess.pid, 'SIGTERM'); } catch { /* ignore */ }
			throw new Error('Server startup timeout');
		}
		await sleep(100);
	}
	const startupTime = Date.now() - t0;
	process.stderr.write('Server ready in ' + startupTime + 'ms\n');
	return { serverProcess, startupTime };
}

async function stopServer(proc) {
	try { process.kill(proc.pid, 'SIGTERM'); } catch { /* ignore */ }
	let exited = false;
	await new Promise((r) => {
		proc.on('exit', () => { exited = true; r(); });
		sleep(10_000).then(() => {
			if (!exited) { try { process.kill(proc.pid, 'SIGKILL'); } catch { /* ignore */ } }
			r();
		});
	});
}

// -------------------------------------------------------------------
// User setup
// -------------------------------------------------------------------

async function getOrCreateBenchUser(port, setupPassword) {
	try {
		const data = JSON.parse(await fs.readFile(BENCH_USER_FILE, 'utf-8'));
		if (data.userId && data.token) {
			const res = await fetch('http://127.0.0.1:' + port + '/api/i', {
				method: 'POST', headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ i: data.token }),
			});
			if (res.ok) return data;
		}
	} catch { /* file not found or invalid */ }

	const body = { username: 'benchadmin', password: 'BenchP4ss!', ...(setupPassword ? { setupPassword } : {}) };
	const res = await fetch('http://127.0.0.1:' + port + '/api/admin/accounts/create', {
		method: 'POST', headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify(body),
	});
	if (!res.ok) throw new Error('Failed to create user: ' + res.status + ' ' + (await res.text()).slice(0, 200));
	const user = await res.json();
	await fs.writeFile(BENCH_USER_FILE, JSON.stringify({ userId: user.id, token: user.token }));
	return { userId: user.id, token: user.token };
}

async function enableFederation(port, token) {
	const res = await fetch('http://127.0.0.1:' + port + '/api/admin/update-meta', {
		method: 'POST', headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({ i: token, federation: 'all' }),
	});
	if (!res.ok) process.stderr.write('  Warning: failed to enable federation: ' + res.status + '\n');
	else process.stderr.write('  Federation enabled\n');
}

// -------------------------------------------------------------------
// Seed remote followers directly into the database.
//
// Each follower lives on a fake host (bench-N.example) with its inbox
// pointing at one of the mock HTTP servers, so notes posted by the
// local admin user trigger real deliver jobs to those inboxes.
// -------------------------------------------------------------------

async function seedRemoteFollowers(dbConfig, localUserId, count) {
	const { default: pg } = await import('pg');
	const client = new pg.Client({
		host: dbConfig.host, port: dbConfig.port,
		database: dbConfig.db, user: dbConfig.user,
		password: dbConfig.pass || undefined,
	});
	await client.connect();

	const now = new Date().toISOString();
	const followerIds = [];

	for (let i = 0; i < count; i++) {
		const id = genId();
		const port = MOCK_PORTS[i % MOCK_PORTS.length];
		const host = 'bench-' + i + '.example';
		const inboxUrl = 'http://127.0.0.1:' + port + '/inbox';
		const uri = 'https://' + host + '/users/follower' + i;
		const username = 'follower' + i;

		await client.query(`
			INSERT INTO "user"
				(id, "createdAt", username, "usernameLower", host, uri, inbox, "sharedInbox",
				 "followersCount", "followingCount", "notesCount")
			VALUES ($1, $2, $3, $4, $5, $6, $7, $7, 0, 0, 0)
			ON CONFLICT DO NOTHING
		`, [id, now, username, username.toLowerCase(), host, uri, inboxUrl]);

		await client.query(`
			INSERT INTO "user_profile" ("userId") VALUES ($1)
			ON CONFLICT DO NOTHING
		`, [id]);

		const followingId = genId();
		await client.query(`
			INSERT INTO following
				(id, "followeeId", "followerId", "isFollowerHibernated", "withReplies",
				 "followerHost", "followerInbox", "followerSharedInbox",
				 "followeeHost", "followeeInbox", "followeeSharedInbox")
			VALUES ($1, $2, $3, false, false, $4, $5, $5, null, null, null)
			ON CONFLICT DO NOTHING
		`, [followingId, localUserId, id, host, inboxUrl]);

		followerIds.push(id);
		process.stderr.write('  Remote follower ' + i + ': ' + host + ' → ' + inboxUrl + '\n');
	}

	// Update the local user's cached follower count
	await client.query(
		`UPDATE "user" SET "followersCount" = "followersCount" + $1 WHERE id = $2`,
		[count, localUserId],
	);

	await client.end();
	process.stderr.write('  Seeded ' + count + ' remote followers\n');
	return followerIds;
}

// -------------------------------------------------------------------
// Latency percentiles
// -------------------------------------------------------------------

function percentile(sorted, p) {
	if (sorted.length === 0) return 0;
	return sorted[Math.min(Math.floor(sorted.length * p / 100), sorted.length - 1)];
}

// -------------------------------------------------------------------
// Single benchmark run
// -------------------------------------------------------------------

async function runBenchmark(config, targetRate, serverPid, token, port, mockServer) {
	const mode = config.nats ? 'nats-relay' : 'bullmq-only';
	const expectedDeliverPerSec = targetRate * FOLLOWER_COUNT;
	process.stderr.write('\n--- ' + mode + ' | target=' + targetRate + ' notes/sec'
		+ ' (≈' + expectedDeliverPerSec + ' deliver jobs/sec)'
		+ ' | duration=' + DURATION_MS + 'ms ---\n');

	const redisConfig = config.redisForJobQueue ?? config.redis;
	const deliverQueue = new Queue('deliver', buildQueueOptions(config, 'deliver'));
	await sleep(SETTLE_TIME);
	mockServer.resetRequestCount();

	const redisBefore = await getRedisStats(redisConfig);
	const procSamples = [];
	let notesSucceeded = 0;
	let notesFailed = 0;
	const latencies = [];
	let maxQueueActive = 0;
	let queueActiveSum = 0, queueActiveSamples = 0;

	const monitorInterval = setInterval(async () => {
		const s = await sampleProcessStats(serverPid);
		if (s) procSamples.push(s);
		try {
			const counts = await deliverQueue.getJobCounts('active', 'waiting');
			if (counts.active > maxQueueActive) maxQueueActive = counts.active;
			queueActiveSum += counts.active;
			queueActiveSamples++;
		} catch { /* ignore */ }
	}, SAMPLE_INTERVAL_MS);

	// Token-bucket paced posting: 10ms tick, release (rate × 0.01) tokens per tick.
	const TICK_MS = 10;
	const tokensPerTick = targetRate * TICK_MS / 1000;
	let tokenBucket = 0;
	let inFlight = 0;
	const startTime = Date.now();
	const totalExpectedNotes = Math.round(targetRate * DURATION_MS / 1000);

	await new Promise((resolveRun) => {
		const tick = setInterval(() => {
			if (Date.now() - startTime >= DURATION_MS) {
				clearInterval(tick);
				const drain = setInterval(() => {
					if (inFlight === 0) { clearInterval(drain); resolveRun(); }
				}, 50);
				return;
			}
			tokenBucket += tokensPerTick;
			while (tokenBucket >= 1 && inFlight < CONCURRENCY) {
				tokenBucket -= 1;
				inFlight++;
				const t0 = Date.now();
				fetch('http://127.0.0.1:' + port + '/api/notes/create', {
					method: 'POST',
					headers: { 'Content-Type': 'application/json' },
					body: JSON.stringify({
						i: token,
						text: 'bench ' + Date.now().toString(36),
						visibility: 'public',
						localOnly: false,
					}),
				}).then(async (res) => {
					latencies.push(Date.now() - t0);
					if (res.ok) notesSucceeded++; else notesFailed++;
				}).catch(() => {
					latencies.push(Date.now() - t0);
					notesFailed++;
				}).finally(() => { inFlight--; });
			}
		}, TICK_MS);
	});

	const notePostTime = Date.now() - startTime;
	process.stderr.write('  Note posting done. Waiting for deliver queue to drain...\n');

	// Wait for all triggered deliver jobs to complete (deliver to mock HTTP)
	const totalExpectedDelivers = notesSucceeded * FOLLOWER_COUNT;
	let timedOut = false;
	const DELIVER_TIMEOUT = 600_000;
	while (mockServer.getRequestCount() < totalExpectedDelivers) {
		await sleep(200);
		if (Date.now() - startTime > DELIVER_TIMEOUT) {
			process.stderr.write('  warning: timeout waiting for deliver drain (got '
				+ mockServer.getRequestCount() + '/' + totalExpectedDelivers + ')\n');
			timedOut = true;
			break;
		}
	}

	const totalTime = Date.now() - startTime;
	clearInterval(monitorInterval);

	const redisAfter = await getRedisStats(redisConfig);
	await deliverQueue.close();

	const proc = computeProcessStats(procSamples);
	latencies.sort((a, b) => a - b);
	const noteThroughput = Math.round(notesSucceeded / (notePostTime / 1000));
	const deliverThroughput = Math.round(mockServer.getRequestCount() / (totalTime / 1000));
	const avgQueueActive = queueActiveSamples > 0
		? Math.round(queueActiveSum / queueActiveSamples * 100) / 100 : 0;

	const result = {
		mode,
		targetNoteRatePerSec: targetRate,
		followerCount: FOLLOWER_COUNT,
		mockDelayMs: MOCK_DELAY_MS,
		durationMs: totalTime,
		notesSucceeded,
		notesFailed,
		noteThroughputPerSec: noteThroughput,
		noteSaturationPct: Math.round(noteThroughput / targetRate * 100),
		deliverJobsTriggered: totalExpectedDelivers,
		deliverJobsCompleted: mockServer.getRequestCount(),
		deliverThroughputPerSec: deliverThroughput,
		timedOut,
		latencyP50Ms: percentile(latencies, 50),
		latencyP95Ms: percentile(latencies, 95),
		latencyP99Ms: percentile(latencies, 99),
		maxQueueActiveSize: maxQueueActive,
		avgQueueActiveSize: avgQueueActive,
		avgCpuPercent: proc.avgCpuPercent,
		avgRssMb: proc.avgRssMb,
		peakRssMb: proc.peakRssMb,
		redisCpuSeconds: Math.round((redisAfter.cpuTotal - redisBefore.cpuTotal) * 1000) / 1000,
		sampleCount: procSamples.length,
	};

	process.stderr.write('  Notes:      ' + notesSucceeded + ' posted (' + noteThroughput + '/s, target ' + targetRate + '/s, ' + result.noteSaturationPct + '%)\n');
	process.stderr.write('  Delivers:   ' + result.deliverJobsCompleted + '/' + totalExpectedDelivers + ' (' + deliverThroughput + '/s)\n');
	process.stderr.write('  Latency:    p50=' + result.latencyP50Ms + 'ms  p95=' + result.latencyP95Ms + 'ms  p99=' + result.latencyP99Ms + 'ms\n');
	process.stderr.write('  Queue peak: ' + maxQueueActive + '  avg: ' + avgQueueActive + '\n');
	process.stderr.write('  Avg CPU:    ' + proc.avgCpuPercent + '%  |  Avg RSS: ' + proc.avgRssMb + ' MB  Peak: ' + proc.peakRssMb + ' MB\n');
	process.stderr.write('  Redis CPU:  ' + result.redisCpuSeconds + 's\n');

	return result;
}

// -------------------------------------------------------------------
// Main
// -------------------------------------------------------------------

async function main() {
	const { config, configPath } = await readConfig();
	const port = config.port ?? 61812;
	const mode = config.nats ? 'nats-relay' : 'bullmq-only';
	process.stderr.write('Note → deliver throughput benchmark\n');
	process.stderr.write('Mode:           ' + mode + '\n');
	process.stderr.write('Target rates:   ' + BENCHMARK_RATES.join(', ') + ' notes/sec\n');
	process.stderr.write('Follower count: ' + FOLLOWER_COUNT + ' remote followers per local user\n');
	process.stderr.write('Duration:       ' + DURATION_MS + 'ms per rate\n');
	process.stderr.write('Inbox delay:    ' + MOCK_DELAY_MS + 'ms\n');

	const originalConfig = await patchConfigForBenchmark(configPath);
	const mockServer = await startMockServers(MOCK_DELAY_MS, MOCK_PORTS);

	// NODE_ENV=test resets the DB on each server start, so clear persisted user
	try { await fs.unlink(BENCH_USER_FILE); } catch { /* ignore */ }

	const { serverProcess, startupTime } = await startServer();
	const results = [];

	try {
		const { userId, token } = await getOrCreateBenchUser(port, config.setupPassword);
		await enableFederation(port, token);

		const dbConfig = config.db;
		await seedRemoteFollowers(dbConfig, userId, FOLLOWER_COUNT);

		for (const rate of BENCHMARK_RATES) {
			const result = await runBenchmark(config, rate, serverProcess.pid, token, port, mockServer);
			results.push({ ...result, startupTimeMs: startupTime });
		}
	} finally {
		await stopServer(serverProcess);
		await mockServer.close();
		await restoreConfig(configPath, originalConfig);
		process.stderr.write('Restored original config\n');
	}

	console.log(JSON.stringify({
		timestamp: new Date().toISOString(),
		mode,
		followerCount: FOLLOWER_COUNT,
		mockDelayMs: MOCK_DELAY_MS,
		durationPerRateMs: DURATION_MS,
		runs: results,
	}, null, 2));
}

main().catch((err) => {
	console.error(JSON.stringify({ error: err.message, stack: err.stack, timestamp: new Date().toISOString() }));
	process.exit(1);
});
