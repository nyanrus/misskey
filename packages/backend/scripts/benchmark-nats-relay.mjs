/*
 * SPDX-FileCopyrightText: syuilo and misskey-project
 * SPDX-License-Identifier: AGPL-3.0-only
 */

/**
 * Benchmark: Misskey queue throughput & BullMQ ACTIVE list size.
 *
 * Starts a real Misskey backend server with a mock HTTP target, pushes
 * deliver jobs through the BullMQ deliver queue, and measures:
 *   - Throughput (jobs/sec)
 *   - Peak & avg ACTIVE list size (the O(n) LREM bottleneck indicator)
 *   - Avg CPU usage (%) of the server process
 *   - Avg / Peak RSS memory (MB) of the server process
 *   - Redis CPU seconds consumed during the benchmark
 *
 * A mock HTTP server simulates federation delivery delay so jobs stay
 * in the ACTIVE list long enough to demonstrate the LREM bottleneck.
 *
 * Runs benchmarks for each job count in BENCHMARK_JOB_COUNTS, restarting
 * the server between runs for clean measurements.
 *
 * Usage: node scripts/benchmark-nats-relay.mjs
 *
 * Environment variables:
 *   BENCHMARK_JOB_COUNTS - comma-separated job counts (default: 1000,5000,10000,50000)
 *   BENCHMARK_DELAY_MS   - mock HTTP response delay in ms (default: 100)
 *
 * Outputs JSON to stdout (like measure-memory.mjs).
 */

import { fork, execSync } from 'node:child_process';
import { createServer } from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join, resolve } from 'node:path';
import { platform } from 'node:os';
import { createHash } from 'node:crypto';
import * as fs from 'node:fs/promises';
import { Queue } from 'bullmq';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const JOB_COUNTS = (process.env.BENCHMARK_JOB_COUNTS ?? '1000,5000,10000,50000')
	.split(',').map(s => parseInt(s.trim()));
const MOCK_DELAY_MS = parseInt(process.env.BENCHMARK_DELAY_MS ?? '1000');
// Run 10 mock servers on different ports
// → 10 separate pools × 256 sockets = 2560 total
const MOCK_PORTS = Array.from({ length: 10 }, (_, i) => 19199 + i);
const STARTUP_TIMEOUT = 120_000;
const BENCHMARK_TIMEOUT = 600_000;
const SETTLE_TIME = 5_000;
const SAMPLE_INTERVAL_MS = 200;

// -------------------------------------------------------------------
// Seeded PRNG (mulberry32) — deterministic delays for reproducibility
// -------------------------------------------------------------------

function mulberry32(seed) {
	return function() {
		seed |= 0; seed = seed + 0x6D2B79F5 | 0;
		let t = Math.imul(seed ^ seed >>> 15, 1 | seed);
		t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t;
		return ((t ^ t >>> 14) >>> 0) / 4294967296;
	};
}

const PRNG_SEED = 42;

// -------------------------------------------------------------------
// Mock HTTP servers — simulate federation delivery targets
// Delay is randomized ±50% around the base delay using a seeded PRNG.
// Multiple servers on different ports to avoid per-host maxSockets limits.
// -------------------------------------------------------------------

function startMockServers(baseDelayMs, ports) {
	let totalRequestCount = 0;
	const servers = [];
	return new Promise((resolve) => {
		let started = 0;
		for (const port of ports) {
			const rng = mulberry32(PRNG_SEED + port);
			const server = createServer((req, resp) => {
				totalRequestCount++;
				req.resume();
				req.on('end', () => {
					const delay = Math.round(baseDelayMs * (0.5 + rng()));
					globalThis.setTimeout(() => {
						resp.writeHead(202);
						resp.end('accepted');
					}, delay);
				});
			});
			// Match HTTP agent's keepAliveMsecs to avoid "socket hang up"
			server.keepAliveTimeout = 60_000;
			servers.push(server);
			server.listen(port, '127.0.0.1', () => {
				started++;
				if (started === ports.length) {
					process.stderr.write('Mock HTTP servers on ports ' + ports[0] + '-' + ports[ports.length - 1] + ' (baseDelay=' + baseDelayMs + 'ms)\n');
					const handle = {
						getRequestCount: () => totalRequestCount,
						resetRequestCount: () => { totalRequestCount = 0; },
						close: () => Promise.all(servers.map(s => new Promise(r => s.close(r)))),
					};
					resolve(handle);
				}
			});
		}
	});
}

// -------------------------------------------------------------------
// Process stats sampling (CPU ticks + RSS)
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
			const rssKb = rssMatch ? parseInt(rssMatch[1]) : 0;
			return { cpuTicks, rssKb, wallMs: now };
		} catch { return null; }
	} else {
		try {
			const out = execSync('ps -o rss=,cputime= -p ' + pid, { encoding: 'utf-8' }).trim();
			const [rssStr, timeStr] = out.split(/\s+/);
			const rssKb = parseInt(rssStr) || 0;
			const parts = (timeStr ?? '0:00:00').split(':').map(Number);
			const totalSec = parts.length === 3
				? parts[0] * 3600 + parts[1] * 60 + parts[2]
				: parts[0] * 60 + parts[1];
			return { cpuTicks: Math.round(totalSec * 100), rssKb, wallMs: now };
		} catch { return null; }
	}
}

function computeProcessStats(samples) {
	const CLK_TCK = 100;
	const valid = samples.filter(Boolean);
	if (valid.length < 2) return { avgCpuPercent: 0, avgRssMb: 0, peakRssMb: 0 };

	const first = valid[0], last = valid[valid.length - 1];
	const wallSec = (last.wallMs - first.wallMs) / 1000;
	const avgCpuPercent = wallSec > 0
		? Math.round((last.cpuTicks - first.cpuTicks) / CLK_TCK / wallSec * 100 * 100) / 100
		: 0;

	const rss = valid.map(s => s.rssKb);
	const avgRssMb = Math.round(rss.reduce((a, b) => a + b, 0) / rss.length / 1024 * 100) / 100;
	const peakRssMb = Math.round(Math.max(...rss) / 1024 * 100) / 100;
	return { avgCpuPercent, avgRssMb, peakRssMb };
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
	return { cpuSys, cpuUser, cpuTotal: Math.round((cpuSys + cpuUser) * 1000) / 1000 };
}

// -------------------------------------------------------------------
// Config & Queue helpers
// -------------------------------------------------------------------

async function readConfig() {
	const builtDir = resolve(__dirname, '../../../built');
	const testPath = resolve(builtDir, '._config_.json');
	const normalPath = resolve(builtDir, '.config.json');
	let configPath;
	try { await fs.access(testPath); configPath = testPath; } catch { configPath = normalPath; }
	return { config: JSON.parse(await fs.readFile(configPath, 'utf-8')), configPath };
}

/**
 * Temporarily patch the config file so the server allows connections
 * to 127.0.0.1 (our mock HTTP target). Without this, the SSRF protection
 * in HttpRequestService blocks loopback addresses and deliver jobs fail
 * silently without ever hitting the mock server.
 */
async function patchConfigForBenchmark(configPath) {
	const original = await fs.readFile(configPath, 'utf-8');
	const config = JSON.parse(original);
	if (!config.allowedPrivateNetworks) {
		config.allowedPrivateNetworks = [];
	}
	if (!config.allowedPrivateNetworks.includes('127.0.0.0/8')) {
		config.allowedPrivateNetworks.push('127.0.0.0/8');
	}
	await fs.writeFile(configPath, JSON.stringify(config, null, '\t'));
	return original;
}

async function restoreConfig(configPath, originalContent) {
	await fs.writeFile(configPath, originalContent);
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
// User creation — needed so signedPost has a real keypair
// Persists user ID to a temp file so the second benchmark run
// (same DB, different config) can reuse the same user.
// -------------------------------------------------------------------

const BENCH_USER_FILE = resolve(__dirname, '../../../.bench-user.json');

async function loadPersistedUser(port) {
	try {
		const data = JSON.parse(await fs.readFile(BENCH_USER_FILE, 'utf-8'));
		if (data.userId && data.token) {
			// Verify the token is still valid (DB may have been reset)
			const res = await fetch('http://127.0.0.1:' + port + '/api/i', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ i: data.token }),
			});
			if (res.ok) {
				process.stderr.write('  Reusing persisted user: ' + data.userId + '\n');
				return { userId: data.userId, token: data.token };
			}
			process.stderr.write('  Persisted token invalid (status ' + res.status + '), will re-create user\n');
		}
	} catch { /* file doesn't exist or server not ready */ }
	return null;
}

async function persistUser(userId, token) {
	await fs.writeFile(BENCH_USER_FILE, JSON.stringify({ userId, token }));
}

async function getOrCreateBenchUser(port, setupPassword) {
	// Check for persisted user from a previous benchmark run
	const persisted = await loadPersistedUser(port);
	if (persisted) return persisted;

	// Try initial admin creation (works on fresh DB)
	try {
		const body = {
			username: 'benchadmin',
			password: 'BenchP4ss!',
			...(setupPassword ? { setupPassword } : {}),
		};
		process.stderr.write('  Admin create attempt: port=' + port + ' setupPassword=' + (setupPassword ? 'yes(' + setupPassword.length + ' chars)' : 'none') + '\n');
		const res = await fetch('http://127.0.0.1:' + port + '/api/admin/accounts/create', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(body),
		});
		const text = await res.text();
		process.stderr.write('  Admin create response: ' + res.status + ' ' + text.slice(0, 500) + '\n');
		if (res.ok) {
			const user = JSON.parse(text);
			process.stderr.write('  Created admin: ' + user.id + '\n');
			await persistUser(user.id, user.token);
			return { userId: user.id, token: user.token };
		}
	} catch (err) {
		process.stderr.write('  Admin create error: ' + err.message + '\n');
	}

	// Try signing in as existing admin and creating a user via admin API
	try {
		process.stderr.write('  Trying admin sign-in...\n');
		const signInRes = await fetch('http://127.0.0.1:' + port + '/api/signin', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ username: 'benchadmin', password: 'BenchP4ss!' }),
		});
		const signInText = await signInRes.text();
		process.stderr.write('  Sign-in response: ' + signInRes.status + ' ' + signInText.slice(0, 300) + '\n');
		if (signInRes.ok) {
			const signIn = JSON.parse(signInText);
			const token = signIn.i ?? signIn.token;
			if (token) {
				// Use admin token to get own user info
				const meRes = await fetch('http://127.0.0.1:' + port + '/api/i', {
					method: 'POST',
					headers: { 'Content-Type': 'application/json' },
					body: JSON.stringify({ i: token }),
				});
				if (meRes.ok) {
					const me = await meRes.json();
					process.stderr.write('  Signed in as admin: ' + me.id + '\n');
					await persistUser(me.id, token);
					return { userId: me.id, token };
				}
			}
		}
	} catch (err) {
		process.stderr.write('  Admin sign-in error: ' + err.message + '\n');
	}

	// Try regular signup (only works in NODE_ENV=test or with registration enabled)
	try {
		process.stderr.write('  Signup attempt...\n');
		const res = await fetch('http://127.0.0.1:' + port + '/api/signup', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				username: 'bench' + Date.now().toString(36),
				password: 'BenchP4ss!',
			}),
		});
		const text = await res.text();
		process.stderr.write('  Signup response: ' + res.status + ' ' + text.slice(0, 500) + '\n');
		if (res.ok) {
			const user = JSON.parse(text);
			process.stderr.write('  Created user: ' + user.id + '\n');
			await persistUser(user.id, user.token);
			return { userId: user.id, token: user.token };
		}
	} catch (err) {
		process.stderr.write('  Signup error: ' + err.message + '\n');
	}

	throw new Error('Failed to create bench user — check server logs');
}

// -------------------------------------------------------------------
// Enable federation so deliver jobs actually make HTTP requests
// -------------------------------------------------------------------

async function enableFederation(port, token) {
	process.stderr.write('  Enabling federation (mode=all)...\n');
	const res = await fetch('http://127.0.0.1:' + port + '/api/admin/update-meta', {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({ i: token, federation: 'all' }),
	});
	if (!res.ok) {
		const text = await res.text();
		process.stderr.write('  Warning: failed to enable federation: ' + res.status + ' ' + text.slice(0, 200) + '\n');
	} else {
		process.stderr.write('  Federation enabled\n');
	}
}

// -------------------------------------------------------------------
// Single benchmark run
// -------------------------------------------------------------------

async function runBenchmark(config, jobCount, serverPid, userId, mockServer) {
	const mode = config.nats ? 'nats-relay' : 'bullmq-only';
	process.stderr.write('\n--- ' + mode + ' | ' + jobCount + ' jobs | delay=' + MOCK_DELAY_MS + 'ms ---\n');
	const redisConfig = config.redisForJobQueue ?? config.redis;
	const deliverQueue = new Queue('deliver', buildQueueOptions(config, 'deliver'));
	await sleep(SETTLE_TIME);
	// Reset after settle so any leftover in-flight NATS deliveries from the
	// previous run that arrive during the settle window are discarded.
	mockServer.resetRequestCount();

	// Build deliver jobs targeting the mock HTTP server
	const content = JSON.stringify({
		'@context': 'https://www.w3.org/ns/activitystreams',
		type: 'Create',
		actor: config.url + '/users/' + userId,
		object: { type: 'Note', content: 'benchmark' },
	});
	const digest = 'SHA-256=' + createHash('sha256').update(content).digest('base64');

	const jobs = Array.from({ length: jobCount }, (_, i) => ({
		name: 'bench-' + i,
		data: {
			user: { id: userId },
			content,
			digest,
			to: `http://127.0.0.1:${MOCK_PORTS[i % MOCK_PORTS.length]}/inbox`,
			isSharedInbox: false,
		},
		opts: { attempts: 1, removeOnComplete: true, removeOnFail: false },
	}));

	// Monitoring: ACTIVE list + process stats
	let maxActive = 0;
	let activeSum = 0, activeSampleCount = 0;
	const procSamples = [];
	const monitorInterval = setInterval(async () => {
		try {
			const counts = await deliverQueue.getJobCounts('active', 'waiting');
			if (counts.active > maxActive) maxActive = counts.active;
			activeSum += counts.active;
			activeSampleCount++;
		} catch { /* ignore */ }
		const s = await sampleProcessStats(serverPid);
		if (s) procSamples.push(s);
	}, SAMPLE_INTERVAL_MS);

	// Snapshot Redis CPU before
	const redisBefore = await getRedisStats(redisConfig);

	// Push all jobs
	const startTime = Date.now();
	await deliverQueue.addBulk(jobs);
	const enqueueTime = Date.now() - startTime;
	process.stderr.write('  Enqueued in ' + enqueueTime + 'ms\n');

	// Wait for drain
	let timedOut = false;
	while (true) {
		await sleep(200);
		const counts = await deliverQueue.getJobCounts('active', 'waiting', 'failed');
		if (counts.active + counts.waiting === 0) break;
		if (Date.now() - startTime > BENCHMARK_TIMEOUT) {
			process.stderr.write('  warning: timeout waiting for BullMQ drain\n');
			timedOut = true;
			break;
		}
	}

	// In NATS relay mode, BullMQ jobs complete immediately after publishing
	// to NATS. The actual HTTP delivery happens asynchronously via NATS
	// consumers. Wait for mock server to receive all expected requests.
	if (mode === 'nats-relay' && !timedOut) {
		process.stderr.write('  BullMQ drained, waiting for NATS consumer delivery...\n');
		while (mockServer.getRequestCount() < jobCount) {
			await sleep(200);
			if (Date.now() - startTime > BENCHMARK_TIMEOUT) {
				process.stderr.write('  warning: timeout waiting for NATS delivery (got ' + mockServer.getRequestCount() + '/' + jobCount + ')\n');
				timedOut = true;
				break;
			}
		}
	}

	const totalTime = Date.now() - startTime;
	clearInterval(monitorInterval);

	// Snapshot Redis CPU after
	const redisAfter = await getRedisStats(redisConfig);

	const finalCounts = await deliverQueue.getJobCounts('active', 'waiting', 'completed', 'failed', 'delayed');

	// Sample a few failed job errors for diagnostics
	let failureSample = [];
	if (finalCounts.failed > 0) {
		try {
			const failedJobs = await deliverQueue.getFailed(0, Math.min(3, finalCounts.failed) - 1);
			failureSample = failedJobs.map(j => ({
				id: j.id,
				failedReason: j.failedReason,
				stacktrace: (j.stacktrace ?? []).slice(0, 1),
			}));
		} catch { /* ignore */ }
	}

	await deliverQueue.close();

	const mockRequestCount = mockServer.getRequestCount();
	// In NATS mode, BullMQ jobs all "succeed" (relay), so use mock HTTP hits
	// as the real delivery success count for accurate throughput measurement.
	const jobsSucceeded = mode === 'nats-relay'
		? mockRequestCount
		: jobCount - finalCounts.failed - finalCounts.active - finalCounts.waiting;
	const proc = computeProcessStats(procSamples);
	const avgActive = activeSampleCount > 0 ? Math.round(activeSum / activeSampleCount * 100) / 100 : 0;
	const redisCpuDelta = Math.round((redisAfter.cpuTotal - redisBefore.cpuTotal) * 1000) / 1000;

	const result = {
		mode,
		jobCount,
		mockDelayMs: MOCK_DELAY_MS,
		jobsSucceeded,
		jobsFailed: finalCounts.failed,
		failureSample,
		mockHttpRequests: mockRequestCount,
		timedOut,
		enqueueTimeMs: enqueueTime,
		totalTimeMs: totalTime,
		throughputJobsPerSec: Math.round(jobsSucceeded / (totalTime / 1000)),
		maxActiveListSize: maxActive,
		avgActiveListSize: avgActive,
		avgCpuPercent: proc.avgCpuPercent,
		avgRssMb: proc.avgRssMb,
		peakRssMb: proc.peakRssMb,
		redisCpuSeconds: redisCpuDelta,
		sampleCount: procSamples.length,
		finalCounts,
	};

	process.stderr.write('  Succeeded:  ' + jobsSucceeded + '/' + jobCount + ' (failed: ' + finalCounts.failed + ', mock HTTP hits: ' + mockRequestCount + ')\n');
	if (failureSample.length > 0) {
		process.stderr.write('  Failure sample:\n');
		for (const f of failureSample) {
			process.stderr.write('    [' + f.id + '] ' + f.failedReason + '\n');
			if (f.stacktrace.length > 0) {
				process.stderr.write('      ' + f.stacktrace[0].split('\n').slice(0, 3).join('\n      ') + '\n');
			}
		}
	}
	process.stderr.write('  Throughput: ' + result.throughputJobsPerSec + ' jobs/sec\n');
	process.stderr.write('  Max ACTIVE: ' + maxActive + '  |  Avg ACTIVE: ' + avgActive + '\n');
	process.stderr.write('  Avg CPU:    ' + proc.avgCpuPercent + '%\n');
	process.stderr.write('  Avg RSS:    ' + proc.avgRssMb + ' MB  |  Peak: ' + proc.peakRssMb + ' MB\n');
	process.stderr.write('  Redis CPU:  ' + redisCpuDelta + 's\n');

	return result;
}

// -------------------------------------------------------------------
// Main
// -------------------------------------------------------------------

async function main() {
	const { config, configPath } = await readConfig();
	const mode = config.nats ? 'nats-relay' : 'bullmq-only';
	process.stderr.write('Benchmark mode: ' + mode + '\n');
	process.stderr.write('Job counts: ' + JOB_COUNTS.join(', ') + '\n');
	process.stderr.write('Mock delay: ' + MOCK_DELAY_MS + 'ms\n');

	// Patch config to allow connections to 127.0.0.1 (mock server).
	// This is needed when NODE_ENV=production (SSRF protection blocks loopback).
	// In NODE_ENV=test the SSRF check is skipped, but the patch is harmless.
	const originalConfig = await patchConfigForBenchmark(configPath);
	process.stderr.write('Patched config: allowedPrivateNetworks includes 127.0.0.0/8\n');

	// Clear persisted user since NODE_ENV=test wipes DB on each server start
	try { await fs.unlink(BENCH_USER_FILE); } catch { /* ignore */ }

	const mockServer = await startMockServers(MOCK_DELAY_MS, MOCK_PORTS);
	const results = [];

	try {
		for (const jobCount of JOB_COUNTS) {
			const { serverProcess, startupTime } = await startServer();
			try {
				// NODE_ENV=test causes dropSchema+synchronize, so DB is wiped on
				// each server start. Must re-create user every time.
				const user = await getOrCreateBenchUser(
					config.port ?? 61812,
					config.setupPassword,
				);
				const userId = user.userId;
				const userToken = user.token;
				if (userToken) {
					await enableFederation(config.port ?? 61812, userToken);
				}
				const result = await runBenchmark(config, jobCount, serverProcess.pid, userId, mockServer);
				results.push({ ...result, startupTimeMs: startupTime });
			} finally {
				await stopServer(serverProcess);
			}
		}
	} finally {
		// Always restore the original config
		await restoreConfig(configPath, originalConfig);
		process.stderr.write('Restored original config\n');
	}

	await mockServer.close();

	console.log(JSON.stringify({
		timestamp: new Date().toISOString(),
		mode,
		mockDelayMs: MOCK_DELAY_MS,
		runs: results,
	}, null, 2));
}

main().catch((err) => {
	console.error(JSON.stringify({ error: err.message, stack: err.stack, timestamp: new Date().toISOString() }));
	process.exit(1);
});
