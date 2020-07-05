const log = require('./log');
const { serializeError } = require('serialize-error');

let workers = {};

async function start(config) {


}

async function stop() {
    for (const w of Object.values(workers)) {
        await w.stop().catch(e => log.log('warning', 'Cannot stop worker', serializeError(e)))
    }
}