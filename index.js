
const { serializeError } = require('serialize-error');
const yaml = require('js-yaml');
const fs = require('fs');
const { Defaults } = require('redis-request-broker');


const log = require('./log');
const contests = require('./contests');
let contest;

async function start() {
    // Load config
    let config;
    try {
        config = yaml.safeLoad(fs.readFileSync('config.yaml', 'utf8'));
    } catch (e) {
        console.error('Cannot load config file. Exiting.');
        console.error(e);
        process.exit(1);
    }

    // Set rrb defaults
    Defaults.setDefaults({
        redis: config.redis
    });

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
    await log.log('notice', 'Shutting down...');
    await contest.stop();
    await log.stop();
    console.log('Shutodwn complete.');
}

start();

process.on('SIGINT', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);