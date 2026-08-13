'use strict';

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const readline = require('readline');

const XP_BASE = 327680000;
const XP_PER_LEVEL = 5000;

const DEFAULT_CONFIG = {
    credentialsFile: '../logpass.txt',
    maFileDirectories: ['../maFiles'],
    authCacheFile: './data/client-refresh-tokens.json',
    steamDataDirectory: './data/steam-user',
    profileRequestTimeoutMs: 20000,
    minRequestIntervalMs: 1200,
    reconnectBaseDelayMs: 5000,
    reconnectMaxDelayMs: 60000
};

function writeStderr(message) {
    process.stderr.write(`[stats] ${message}\n`);
}

function writeProtocol(message) {
    process.stdout.write(`${JSON.stringify(message)}\n`);
}

function printUsage() {
    console.log('Usage:');
    console.log('  node stats.js --stdio --account worker_name --config stats-worker-config.json');
    console.log();
    console.log('The process accepts JSON Lines on stdin and writes JSON Lines to stdout.');
    console.log('It is intended to be launched by stats_server/server.py, not manually.');
}

function parseArgs(argv) {
    const result = {
        account: '',
        config: path.join(__dirname, 'stats-worker-config.json'),
        help: false,
        stdio: false
    };

    for (let index = 0; index < argv.length; index += 1) {
        const argument = argv[index];

        if (argument === '--help' || argument === '-h') {
            result.help = true;
            continue;
        }
        if (argument === '--stdio') {
            result.stdio = true;
            continue;
        }
        if (argument === '--account' && argv[index + 1]) {
            result.account = argv[index + 1].trim();
            index += 1;
            continue;
        }
        if (argument === '--config' && argv[index + 1]) {
            result.config = argv[index + 1].trim();
            index += 1;
            continue;
        }

        throw new Error(`Unknown or incomplete argument: ${argument}`);
    }

    return result;
}

function readJson(filePath) {
    try {
        return JSON.parse(fs.readFileSync(filePath, 'utf8'));
    } catch {
        return null;
    }
}

function writeJsonAtomically(filePath, value) {
    const directory = path.dirname(filePath);
    fs.mkdirSync(directory, { recursive: true });

    const temporaryPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
    fs.writeFileSync(temporaryPath, JSON.stringify(value, null, 2), {
        encoding: 'utf8',
        mode: 0o600
    });
    fs.renameSync(temporaryPath, filePath);

    if (process.platform !== 'win32') {
        fs.chmodSync(filePath, 0o600);
    }
}

function mergeConfig(base, override) {
    const result = { ...base };

    for (const [key, value] of Object.entries(override || {})) {
        if (
            value &&
            typeof value === 'object' &&
            !Array.isArray(value) &&
            result[key] &&
            typeof result[key] === 'object' &&
            !Array.isArray(result[key])
        ) {
            result[key] = mergeConfig(result[key], value);
        } else {
            result[key] = value;
        }
    }

    return result;
}

function resolveConfigPath(configDirectory, value) {
    const candidate = String(value || '').trim();
    return path.isAbsolute(candidate)
        ? candidate
        : path.resolve(configDirectory, candidate);
}

function assertPositiveInteger(value, name, allowZero = false) {
    const number = Number(value);
    if (!Number.isInteger(number) || number < 0 || (!allowZero && number === 0)) {
        throw new Error(`${name} must be a ${allowZero ? 'non-negative' : 'positive'} integer.`);
    }
    return number;
}

function loadConfig(configPath) {
    const absoluteConfigPath = path.resolve(configPath);
    const supplied = readJson(absoluteConfigPath);

    if (!supplied || typeof supplied !== 'object' || Array.isArray(supplied)) {
        throw new Error(`Could not read JSON worker configuration: ${absoluteConfigPath}`);
    }

    const config = mergeConfig(DEFAULT_CONFIG, supplied);
    const configDirectory = path.dirname(absoluteConfigPath);
    config.credentialsFile = resolveConfigPath(configDirectory, config.credentialsFile);
    config.maFileDirectories = Array.isArray(config.maFileDirectories)
        ? config.maFileDirectories.map((directory) => resolveConfigPath(configDirectory, directory))
        : [];
    config.authCacheFile = resolveConfigPath(configDirectory, config.authCacheFile);
    config.steamDataDirectory = resolveConfigPath(configDirectory, config.steamDataDirectory);

    if (!config.maFileDirectories.length) {
        throw new Error('maFileDirectories must contain at least one directory.');
    }

    for (const settingName of [
        'profileRequestTimeoutMs',
        'minRequestIntervalMs',
        'reconnectBaseDelayMs',
        'reconnectMaxDelayMs'
    ]) {
        config[settingName] = assertPositiveInteger(config[settingName], settingName, settingName === 'minRequestIntervalMs');
    }

    return config;
}

function loadCredentials(credentialsFile) {
    const credentials = new Map();
    let content = '';

    try {
        content = fs.readFileSync(credentialsFile, 'utf8');
    } catch {
        return credentials;
    }

    for (const rawLine of content.split(/\r?\n/)) {
        const line = rawLine.trim();
        const separator = line.indexOf(':');
        if (!line || separator <= 0) {
            continue;
        }

        const accountName = line.slice(0, separator).trim();
        const password = line.slice(separator + 1).trim();
        if (accountName && password) {
            credentials.set(accountName, password);
        }
    }

    return credentials;
}

function findMaFile(accountName, maFileDirectories) {
    for (const directory of maFileDirectories) {
        let fileNames = [];
        try {
            fileNames = fs.readdirSync(directory);
        } catch {
            continue;
        }

        for (const fileName of fileNames) {
            if (!fileName.endsWith('.maFile')) {
                continue;
            }

            const filePath = path.join(directory, fileName);
            const data = readJson(filePath);
            if (data && String(data.account_name || '').trim() === accountName) {
                return { filePath, data };
            }
        }
    }

    return null;
}

function loadTokenCache(cacheFile) {
    const cache = readJson(cacheFile);
    return cache && typeof cache === 'object' && !Array.isArray(cache) ? cache : {};
}

function saveRefreshToken(cacheFile, accountName, refreshToken, steamId) {
    if (!accountName || !refreshToken) {
        return;
    }

    const cache = loadTokenCache(cacheFile);
    const current = cache[accountName] && typeof cache[accountName] === 'object'
        ? cache[accountName]
        : {};
    cache[accountName] = {
        ...current,
        clientRefreshToken: refreshToken,
        steamId: String(steamId || current.steamId || '').trim(),
        updatedAt: new Date().toISOString()
    };
    writeJsonAtomically(cacheFile, cache);
}

function loadAccountContext(accountName, config) {
    const maFile = findMaFile(accountName, config.maFileDirectories);
    if (!maFile) {
        throw new Error(`maFile was not found for worker account ${accountName}.`);
    }

    const credentials = loadCredentials(config.credentialsFile);
    const tokenCache = loadTokenCache(config.authCacheFile);
    const tokenEntry = tokenCache[accountName] && typeof tokenCache[accountName] === 'object'
        ? tokenCache[accountName]
        : {};
    const session = maFile.data.Session && typeof maFile.data.Session === 'object'
        ? maFile.data.Session
        : {};
    const sharedSecret = String(maFile.data.shared_secret || '').trim();
    const password = credentials.get(accountName) || '';
    const refreshToken = String(tokenEntry.clientRefreshToken || '').trim();

    if (!refreshToken && !password) {
        throw new Error(`No local password or saved client refresh token is available for ${accountName}.`);
    }
    if (!sharedSecret && !refreshToken) {
        throw new Error(`Steam Guard shared_secret is missing for ${accountName}.`);
    }

    return {
        accountName,
        password,
        refreshToken,
        sharedSecret,
        steamId: String(session.SteamID || tokenEntry.steamId || '').trim()
    };
}

function generateSteamGuardCode(sharedSecret) {
    const key = Buffer.from(sharedSecret, 'base64');
    const timeSlice = Math.floor(Date.now() / 1000 / 30);
    const buffer = Buffer.alloc(8);
    buffer.writeUInt32BE(0, 0);
    buffer.writeUInt32BE(timeSlice, 4);

    const digest = crypto.createHmac('sha1', key).update(buffer).digest();
    const alphabet = '23456789BCDFGHJKMNPQRTVWXY';
    const offset = digest[19] & 0x0F;
    let codePoint = digest.readUInt32BE(offset) & 0x7FFFFFFF;
    let code = '';

    for (let index = 0; index < 5; index += 1) {
        code += alphabet[codePoint % alphabet.length];
        codePoint = Math.floor(codePoint / alphabet.length);
    }

    return code;
}

function nextSteamGuardDelayMs() {
    const nowSeconds = Math.floor(Date.now() / 1000);
    return (30 - (nowSeconds % 30) + 1) * 1000;
}

function delay(milliseconds) {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function withTimeout(promise, timeoutMilliseconds, label) {
    let timeoutId = null;
    const timeout = new Promise((_, reject) => {
        timeoutId = setTimeout(() => reject(new Error(`${label} timed out.`)), timeoutMilliseconds);
    });

    return Promise.race([promise, timeout]).finally(() => clearTimeout(timeoutId));
}

function parseCurrentXp(profile, steamId) {
    const rawXp = Number(profile && profile.player_cur_xp);
    const rawLevel = Number(profile && profile.player_level);

    if (!Number.isFinite(rawXp)) {
        throw new Error(
            `Steam did not return player_cur_xp. ` +
            `steamId=${steamId}, rawXp=${profile && profile.player_cur_xp}, ` +
            `playerLevel=${profile && profile.player_level}`
        );
    }

    const currentXp = rawXp - XP_BASE;

    if (currentXp < 0 || currentXp > XP_PER_LEVEL) {
        throw new Error(
            `Unsupported CS2 XP: ` +
            `steamId=${steamId}, ` +
            `rawXp=${rawXp}, ` +
            `currentXp=${currentXp}, ` +
            `playerLevel=${Number.isFinite(rawLevel) ? rawLevel : 'null'}, ` +
            `XP_BASE=${XP_BASE}`
        );
    }

    const profileLevel =
        Number.isFinite(rawLevel)
            ? rawLevel
            : null;

    return {
        steamId,
        currentXp,
        xpPerLevel: XP_PER_LEVEL,
        progressPercent: Number(
            ((currentXp / XP_PER_LEVEL) * 100).toFixed(2)
        ),
        xpRemaining: XP_PER_LEVEL - currentXp,
        profileLevel,
        rawXp,
        fetchedAt: new Date().toISOString()
    };
}

class SteamStatsWorker {
    constructor(context, config, SteamUser, NodeCS2) {
        this.context = context;
        this.config = config;
        this.SteamUser = SteamUser;
        this.NodeCS2 = NodeCS2;
        this.state = 'stopped';
        this.user = null;
        this.cs2 = null;
        this.generation = 0;
        this.reconnectTimer = null;
        this.reconnectAttempt = 0;
        this.lastRequestAt = 0;
        this.pendingRequests = [];
        this.activeRequest = null;
        this.usedPasswordFallback = false;
        this.stopping = false;
        this.loginMode = '';
    }

    start() {
        this.stopping = false;
        this._connect(false);
    }

    stop() {
        this.stopping = true;
        this.generation += 1;
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = null;
        this._failActive(new Error('Worker stopped.'));
        while (this.pendingRequests.length) {
            const request = this.pendingRequests.shift();
            writeProtocol({ type: 'error', id: request.id, error: 'Worker stopped.' });
        }
        this._disposeClients();
        this._setState('stopped');
    }

    enqueue(request) {
        this.pendingRequests.push(request);
        this._processNext();
    }

    _connect(preferPassword) {
        if (this.stopping) {
            return;
        }

        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = null;
        const generation = ++this.generation;
        this._disposeClients();
        this._setState('connecting');

        const user = new this.SteamUser({
            autoRelogin: false,
            renewRefreshTokens: true,
            dataDirectory: path.join(this.config.steamDataDirectory, this.context.accountName)
        });
        const cs2 = new this.NodeCS2(user);
        this.user = user;
        this.cs2 = cs2;
        this.loginMode = '';
        if (!preferPassword) {
            this.usedPasswordFallback = false;
        }

        this._bindEvents(user, cs2, generation);

        try {
            if (!preferPassword && this.context.refreshToken) {
                this.loginMode = 'refresh-token';
                const details = { refreshToken: this.context.refreshToken };
                if (this.context.steamId) {
                    details.steamID = this.context.steamId;
                }
                user.logOn(details);
                return;
            }

            if (!this.context.password) {
                throw new Error('No password is available for fallback Steam login.');
            }

            this.loginMode = 'password';
            user.logOn({ accountName: this.context.accountName, password: this.context.password });
        } catch (error) {
            this._scheduleReconnect(error, generation);
        }
    }

    _bindEvents(user, cs2, generation) {
        const isCurrent = () => !this.stopping && generation === this.generation;

        user.on('refreshToken', (refreshToken) => {
            if (!isCurrent()) {
                return;
            }
            try {
                const steamId = user.steamID ? user.steamID.getSteamID64() : this.context.steamId;
                saveRefreshToken(this.config.authCacheFile, this.context.accountName, refreshToken, steamId);
                this.context.refreshToken = refreshToken;
            } catch (error) {
                writeStderr(`Could not save refreshed token for ${this.context.accountName}: ${error.message}`);
            }
        });

        user.on('steamGuard', (domain, callback, lastCodeWrong) => {
            if (!isCurrent()) {
                return;
            }
            const waitMilliseconds = lastCodeWrong ? nextSteamGuardDelayMs() : 0;
            setTimeout(() => {
                if (!isCurrent()) {
                    return;
                }
                try {
                    callback(generateSteamGuardCode(this.context.sharedSecret));
                } catch (error) {
                    this._scheduleReconnect(error, generation);
                }
            }, waitMilliseconds);
        });

        user.on('loggedOn', () => {
            if (isCurrent()) {
                user.gamesPlayed([730]);
            }
        });

        user.on('appLaunched', (appId) => {
            if (isCurrent() && Number(appId) === 730) {
                cs2.helloGC();
            }
        });

        cs2.on('connectedToGC', () => {
            if (!isCurrent()) {
                return;
            }
            this.reconnectAttempt = 0;
            this._setState('ready');
            this._processNext();
        });

        user.on('error', (error) => {
            if (!isCurrent()) {
                return;
            }
            if (this.loginMode === 'refresh-token' && !this.usedPasswordFallback && this.context.password) {
                this.usedPasswordFallback = true;
                setTimeout(() => this._connect(true), 0);
                return;
            }
            this._scheduleReconnect(error, generation);
        });

        user.on('disconnected', (result, message) => {
            if (isCurrent()) {
                this._scheduleReconnect(new Error(message || `Steam disconnected (${result}).`), generation);
            }
        });

        cs2.on('error', (error) => {
            if (isCurrent()) {
                this._scheduleReconnect(error, generation);
            }
        });
    }

    _processNext() {
        if (this.stopping || this.state !== 'ready' || this.activeRequest || !this.pendingRequests.length) {
            return;
        }

        const request = this.pendingRequests.shift();
        this.activeRequest = request;
        this._setState('busy');

        this._fetch(request)
            .then((result) => {
                if (this.activeRequest !== request) {
                    return;
                }
                writeProtocol({ type: 'result', id: request.id, result });
            })
            .catch((error) => {
                if (this.activeRequest !== request) {
                    return;
                }
                this._scheduleReconnect(error);
            })
            .finally(() => {
                if (this.activeRequest === request) {
                    this.activeRequest = null;
                }
                if (!this.stopping && this.state === 'busy') {
                    this._setState('ready');
                }
                this._processNext();
            });
    }

    async _fetch(request) {
        const waitMilliseconds = Math.max(0, this.lastRequestAt + this.config.minRequestIntervalMs - Date.now());
        if (waitMilliseconds > 0) {
            await delay(waitMilliseconds);
        }

        const profile = await withTimeout(
            Promise.resolve(this.cs2.requestPlayersProfile(request.steamId)),
            this.config.profileRequestTimeoutMs,
            `CS2 profile request for ${request.steamId}`
        );
        this.lastRequestAt = Date.now();
        return parseCurrentXp(profile, request.steamId);
    }

    _scheduleReconnect(error, generation = this.generation) {
        if (this.stopping || generation !== this.generation) {
            return;
        }

        this._failActive(error);
        if (this.reconnectTimer) {
            return;
        }
        this._setState('cooldown', error);
        const delayMilliseconds = Math.min(
            this.config.reconnectMaxDelayMs,
            this.config.reconnectBaseDelayMs * (2 ** this.reconnectAttempt)
        );
        this.reconnectAttempt += 1;
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            this._connect(false);
        }, delayMilliseconds);
    }

    _failActive(error) {
        if (!this.activeRequest) {
            return;
        }
        const request = this.activeRequest;
        this.activeRequest = null;
        writeProtocol({ type: 'error', id: request.id, error: String(error.message || error) });
    }

    _disposeClients() {
        const oldUser = this.user;
        this.user = null;
        this.cs2 = null;
        if (oldUser) {
            try {
                oldUser.logOff();
            } catch {}
        }
    }

    _setState(nextState, error = null) {
        if (this.state === nextState && !error) {
            return;
        }
        this.state = nextState;
        writeProtocol({
            type: 'status',
            account: this.context.accountName,
            state: nextState,
            error: error ? String(error.message || error) : null
        });
    }
}

function validateRequest(message) {
    const requestId = String(message && message.id || '').trim();
    const action = String(message && message.action || '').trim();
    const steamId = String(message && message.steamId || '').trim();
    if (!requestId) {
        throw new Error('Request id is required.');
    }
    if (action !== 'getXp') {
        throw new Error('Only getXp action is supported.');
    }
    if (!/^7656\d{13}$/.test(steamId)) {
        throw new Error('steamId must be a 17-digit SteamID64.');
    }
    return { id: requestId, steamId };
}

function run() {
    let args;
    try {
        args = parseArgs(process.argv.slice(2));
    } catch (error) {
        writeStderr(error.message);
        process.exitCode = 2;
        return;
    }

    if (args.help) {
        printUsage();
        return;
    }
    if (!args.stdio || !args.account) {
        writeStderr('--stdio and --account are required.');
        process.exitCode = 2;
        return;
    }

    let config;
    let context;
    let SteamUser;
    let NodeCS2;
    try {
        config = loadConfig(args.config);
        context = loadAccountContext(args.account, config);
        SteamUser = require('steam-user');
        NodeCS2 = require('node-cs2');
    } catch (error) {
        writeStderr(error.message);
        writeProtocol({ type: 'status', account: args.account, state: 'failed', error: error.message });
        process.exitCode = 2;
        return;
    }

    const worker = new SteamStatsWorker(context, config, SteamUser, NodeCS2);
    const input = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });

    input.on('line', (line) => {
        let message;
        try {
            message = JSON.parse(line);
            worker.enqueue(validateRequest(message));
        } catch (error) {
            const requestId = message && typeof message === 'object' ? String(message.id || '') : '';
            writeProtocol({ type: 'error', id: requestId, error: String(error.message || error) });
        }
    });

    input.on('close', () => worker.stop());
    process.on('SIGINT', () => worker.stop());
    process.on('SIGTERM', () => worker.stop());
    worker.start();
}

run();