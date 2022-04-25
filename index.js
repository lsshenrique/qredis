'use strict';

const redis = require('redis');
const { createClient } = redis;
const { promisify } = require("util");
const { countScan, delScan } = require("./utils");

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

    ttlAsync(...args) {
        return promisify(this.ttl).bind(this)(...args);
    }

    saddAsync(...args) {
        return promisify(this.sadd).bind(this)(...args);
    }

    smembersAsync(...args) {
        return promisify(this.smembers).bind(this)(...args);
    }

    sremAsync(...args) {
        return promisify(this.srem).bind(this)(...args);
    }

    hsetAsync(...args) {
        return promisify(this.hset).bind(this)(...args);
    }

    hgetAsync(...args) {
        return promisify(this.hget).bind(this)(...args);
    }

    hdelAsync(...args) {
        return promisify(this.hdel).bind(this)(...args);
    }

    hkeysAsync(...args) {
        return promisify(this.hkeys).bind(this)(...args);
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