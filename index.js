
const { serializeError } = require('serialize-error');

const log = require('./log');

async function start() {
    try {
        await log.start();
    }
    catch (e) {
        await log.log('error', 'Failed to start up', serializeError(e));
        await stop();
    }
}

async function stop() {
    await log.log('notice', 'Shutting down...');
    await log.stop();
}

start();

process.on('SIGINT', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);