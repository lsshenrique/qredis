'use strict';

const redis = require('redis');
const { createClient } = redis;
const { promisify } = require("util");
const { countScan, delScan } = require("./utils");
const crypto = require('crypto');

class QRedisClient extends redis.RedisClient {
    constructor(client, options) {
        super(options);
        this._client = client;
        this._options = options || {};

        if (this._options.enableCacheOnRunGet !== false) this._options.enableCacheOnRunGet = true;
    }

    getAsync(...args) {
        return promisify(this.get).bind(this)(...args);
    }

    async getAsObjAsync(...args) {
        const cache = await this.getAsync(...args);
        return cache && JSON.parse(cache);
    }

    setexAsync(...args) {
        return promisify(this.setex).bind(this)(...args);
    }

    setAsync(key, value, timeout) {
        if (typeof value === 'object') value = JSON.stringify(value);
        if (value === null || value === undefined) value = '';

        if (isNaN(timeout)) {
            return promisify(this.set).bind(this)(key, value);
        }

        return this.setexAsync(key, timeout, value);
    }

    expireAsync(...args) {
        return promisify(this.expire).bind(this)(...args);
    }

    async getsetAsync(key, value, timeout) {
        const result = await promisify(this.getset).bind(this)(key, value);

        if (!isNaN(timeout)) {
            await this.expireAsync(key, timeout);
        }

        return result;
    }

    delAsync(...args) {
        return promisify(this.del).bind(this)(...args);
    }

    async runGet(keyInfo, parameters, fnExecuteGet, options = {}) {
        let value = null;

        if (this._options.enableCacheOnRunGet) {
            const key = `${keyInfo.namespace}#${parameters.join('_')}`;

            if (options.asObject) {
                value = await this.getAsObjAsync(key);
            } else {
                value = await this.getAsync(key);
            }

            if (value) {
                // eslint-disable-next-line no-console
                if (this._options.enableLog) console.info('[REDIS]', keyInfo.namespace, `exists in cache`);
            } else {
                // eslint-disable-next-line no-console
                if (this._options.enableLog) console.info('[REDIS]', keyInfo.namespace, `not exists in cache`);

                value = await fnExecuteGet();
                await this.setAsync(key, value, keyInfo.timeout);
            }
        } else {
            value = await fnExecuteGet();
        }

        if (options.parse && typeof options.parse === 'function') {
            value = options.parse(value);
        }

        return value;
    }

    async runGetInt(keyInfo, parameters, fnExecuteGet, options = {}) {
        return await this.runGet(keyInfo, parameters, fnExecuteGet, {
            ...options,
            parse: parseInt,
        });
    }

    async runGetFloat(keyInfo, parameters, fnExecuteGet, options = {}) {
        return await this.runGet(keyInfo, parameters, fnExecuteGet, {
            ...options,
            parse: parseFloat,
        });
    }

    async runGetObj(keyInfo, parameters, fnExecuteGet, options = {}) {
        return await this.runGet(keyInfo, parameters, fnExecuteGet, {
            ...options,
            asObject: true,
        });
    }

    count(pattern, callback) {
        countScan(this, '0', pattern, 0, callback);
    }

    countAsync(pattern) {
        return promisify(this.count).bind(this)(pattern);
    }

    delByPattern(pattern, callback) {
        delScan(this, '0', pattern, 0, callback);
    }

    delByPatternAsync(pattern) {
        return promisify(this.delByPattern).bind(this)(pattern);
    }

    ttl(...args) {
        return promisify(this._client.ttl).bind(this._client)(...args);
    }

    sadd(...args) {
        return promisify(this._client.sadd).bind(this._client)(...args);
    }

    smembers(...args) {
        return promisify(this._client.smembers).bind(this._client)(...args);
    }

    srem(...args) {
        return promisify(this._client.srem).bind(this._client)(...args);
    }

    hset(...args) {
        return promisify(this._client.hset).bind(this._client)(...args);
    }

    hget(...args) {
        return promisify(this._client.hget).bind(this._client)(...args);
    }

    hdel(...args) {
        return promisify(this._client.hdel).bind(this._client)(...args);
    }

    hkeys(...args) {
        return promisify(this._client.hkeys).bind(this._client)(...args);
    }

    eval(...args) {
        return promisify(this._client.eval).bind(this._client)(...args);
    }

    sendCommand(...args) {
        return promisify(this._client.sendCommand).bind(this._client)(...args);
    }

    // https://github.com/coligo-org/simple-redis-mutex
    async mutexLock({ key, timeoutMillis, failAfterMillis, retryTimeMillis }) {

        // eslint-disable-next-line consistent-this
        const client = this;
        const lockValue = crypto.randomBytes(50).toString('hex');

        const acquireLock = new Promise((resolve, reject) => {
            let failTimeoutId = null;
            let attemptTimeoutId = null;

            // Try to acquire the lock, and try again after a while on failure
            function attempt() {
                let clientSetPromise;

                if (timeoutMillis !== null) {
                    clientSetPromise = client.sendCommand("SET", [key, lockValue, 'NX', 'PX', timeoutMillis]);
                } else {
                    clientSetPromise = client.sendCommand("SET", [key, lockValue, 'NX']);
                }

                // Try to set the lock if it does not exist, else try again later, also set a timeout for the lock so it expires
                clientSetPromise.then(response => {
                    if (response === 'OK') {
                        // Clear failure timer if it was set
                        if (failTimeoutId !== null) { clearTimeout(failTimeoutId); }
                        resolve();
                    } else {
                        attemptTimeoutId = setTimeout(attempt, retryTimeMillis);
                    }
                });
            }

            // Set time out to fail acquiring the lock if it's sent
            if (failAfterMillis !== null) {
                failTimeoutId = setTimeout(
                    () => {
                        if (attemptTimeoutId !== null) { clearTimeout(attemptTimeoutId); }
                        reject(new Error(`Lock could not be acquire for ${failAfterMillis} millis`));
                    },
                    failAfterMillis,
                );
            }

            attempt();
        });

        function releaseLock() {
            /*
             * Release the lock only if it has the same lockValue as acquireLock sets it.
             * This will prevent the release of an already released lock.
             *
             * Script source: https://redis.io/commands/set#patterns - Redis official docs
             */
            const luaReleaseScript = `
                if redis.call("get", KEYS[1]) == ARGV[1]
                then
                    return redis.call("del", KEYS[1])
                else
                    return 0
                end
            `;

            // After calling the script, make sure to return void promise.
            return client.eval(luaReleaseScript, 1, key, lockValue).then(() => {});
        }

        await acquireLock;

        return releaseLock;
    }
}


module.exports = redis;

module.exports.createClient = function(options, ...args) {
    const client = createClient(options, ...args);
    const optionsAux = typeof options === 'object' ? options : {};

    const date = new Date();
    const dateFormated = `${date.toLocaleDateString()} ${date.toLocaleTimeString()}`;

    // eslint-disable-next-line no-console
    client.on("ready", () => console.info(`[REDIS] conected on ${optionsAux.host}:${optionsAux.port} at ${dateFormated}`));

    // eslint-disable-next-line no-console
    client.on("error", (error) => console.error('[REDIS]', error));

    return new QRedisClient(client, {
        ...optionsAux,
    });
};

module.exports.TIMER = {
    // seconds
    s: (value) => value,

    // minutes
    m: (value) => 60 * value,

    // hours
    h: (value) => 60 * 60 * value,
};

module.exports.QRedisClient = QRedisClient;