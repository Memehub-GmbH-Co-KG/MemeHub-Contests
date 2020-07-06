const { Publisher } = require('redis-request-broker');
const i = require('./instance');
let client;

module.exports.start = async function start(config) {
    const c = new Publisher(config.rrb.channels.logging);
    await c.connect();
    client = c;
}

module.exports.stop = async function stop() {
    const c = client;
    client = undefined;
    if (c)
        await c.disconnect().catch(e => console.log('Failed to stop logger:', e));
}

module.exports.log = async function log(level, title, data, component = i.component, instance = i.instance) {
    try {
        if (!client)
            return console.log('Cannot send log, as not started jet:', { level, title, data, component, instance });

        const r = await client.publish({ level, title, data, component, instance });
        if (r < 1)
            console.warn('log not received by logger:', { level, title, data, component, instance });
    }
    catch (error) {
        console.log('Failed to send log:', { level, title, data, component, instance });
    }
}
