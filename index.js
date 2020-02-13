'use strict';

const redis = require('redis');
const { createClient } = redis;
const { promisify } = require("util");

class QRedisClient extends redis.RedisClient {
    constructor(client, options) {
        super();
        this._client = client;
        this._options = options;
    }

    getAsync(...args) {
        return promisify(this.get).bind(this)(...args);
    }

    async getAsObjAsync(...args) {
        const cache = await this.getAsync(...args);
        return cache && JSON.parse(cache);
    }

    setexAsync(...args) {
        return promisify(this.setex).bind(this, ...args);
    }

    setAsync(key, value, timeout) {
        if (typeof value === 'object') value = JSON.stringify(value);

        if (isNaN(timeout)) {
            return promisify(this.set).bind(this)(key, value);
        }

        return this.setexAsync(key, timeout, value);
    }
}

module.exports = redis;

module.exports.createClient = function(options, ...args) {
    const client = createClient(options, ...args);
    const optionsAux = typeof options === 'object' ? options : {};

    const date = new Date();
    const dateFormated = `${date.toLocaleDateString()} ${date.toLocaleTimeString()}`;
    // eslint-disable-next-line no-console
    client.on("ready", () => console.error(`[REDIS] conected on ${optionsAux.host}:${optionsAux.port} at ${dateFormated}`));

    // eslint-disable-next-line no-console
    client.on("error", (error) => console.error('[REDIS]', error));

    return new QRedisClient(client, {
        ...optionsAux,
    });
};