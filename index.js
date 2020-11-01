
const { serializeError } = require('serialize-error');
const { Defaults, Subscriber, Client } = require('redis-request-broker');

const log = require('./log');
const contests = require('./contests');
let contest;
let restartSubscriber;

async function start() {
    // Set rrb defaults
    Defaults.setDefaults({
        redis: {
            prefix: process.env.REDIS_PREFIX || 'mh:',
            host: process.env.REDIS_HOST || "mhredis",
            port: process.env.REDIS_PORT || undefined,
            db: process.env.REDIS_DB || undefined,
            password: process.env.REDIS_PASSWORD || undefined
        }
    });

    // Load config
    let config;
    try {
        config = await getConfig();
    } catch (e) {
        console.error('Cannot load config. Exiting.');
        console.error(e);
        process.exit(1);
    }

    // Trigger restart on config change
    restartSubscriber = new Subscriber(config.rrb.channels.config.changed, onConfigChange);
    await restartSubscriber.listen();

    try {
        console.log('Starting up...');
        await log.start(config);
        contest = await contests.build(config);
        await log.log('notice', 'Startup complete.');
    }
    catch (e) {
        await log.log('error', 'Failed to start up', serializeError(e));
        await stop();
    }
}

async function stop() {
    try {
        await log.log('notice', 'Shutting down...');
        restartSubscriber && await restartSubscriber.stop().catch(console.error);
        await contest.stop();
        await log.stop();
        console.log('Shutodwn complete.');
        process.exit(0);
    }
    catch (error) {
        console.error('Failed to shut down.');
        console.error(error);
        process.exit(1);
    }
}


async function restart() {
    await stop();
    await start();
}

async function onConfigChange(keys) {
    if (!Array.isArray(keys))
        restart();

    if (keys.some(k => k.startsWith('redis') || k.startsWith('rrb') || k.startsWith('mongodb')))
        restart();
}


async function getConfig() {
    const client = new Client('config:get', { timeout: 10000 });
    await client.connect();
    const [redis, rrb, mongodb] = await client.request(['redis', 'rrb', 'mongodb']);
    await client.disconnect();
    return { redis, rrb, mongodb };
}

start();

process.on('SIGINT', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);