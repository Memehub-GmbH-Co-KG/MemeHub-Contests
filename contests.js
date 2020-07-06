const { log } = require('./log');
const { serializeError } = require('serialize-error');
const { Worker } = require('redis-request-broker');
const Redis = require("ioredis");
const { MongoClient } = require('mongodb');

module.exports.build = async function build(config) {

    const mongoClient = new MongoClient(config.mongodb.connectionString, { useNewUrlParser: true, useUnifiedTopology: true });
    const workers = {};
    /** @type {import('mongodb').Collection} */
    let collection;

    try {
        await start()
    }
    catch (error) {
        await log('error', 'Failed to start contests.', serializeError(error));
        await stop();
        throw error;
    }

    async function start() {
        // Connect to mongodb
        await mongoClient.connect();
        collection = await mongoClient.db(config.mongodb.database).createCollection(config.mongodb.collection).catch(console.log);
    
        // Start worker
        workers.create = new Worker(config.rrb.queues.contestsCreate, createContest);
        workers.list = new Worker(config.rrb.queues.contestsList, list);
        workers.delete = new Worker(config.rrb.queues.contestsDelete, deleteContest);
        workers.start = new Worker(config.rrb.queues.contestsStart, startContest);
        workers.stop = new Worker(config.rrb.queues.contestsStop, stopContest);
        await workers.create.listen();
        await workers.list.listen();
        await workers.delete.listen();
        await workers.start.listen();
        await workers.stop.listen();
    }

    async function stop() {
        try {
            // Stop worker
            for (const w of Object.values(workers)) {
                await w.stop().catch(e => log.log('warning', 'Cannot stop worker', serializeError(e)))
            }
    
            // Disconnect from mongodb
            await mongoClient.close();
        }
        catch (error) {
            await log.log('warning', 'Failed to stop contets', serializeError(error));
        }
    }

    async function createContest({id, tag, emoji}) {
        try {
            const result = await collection.insertOne({_id: id, tag, emoji, running: false});
            if (!result.result.ok) {
                log('info', 'Cannot create contest: MongoDB response is not ok.', { result, id });
                return false;
            }
            
            if (result.insertedCount !== 1) {
                log('info', 'Response missmatch while creating contest: insertedCount is not 1.', { result, id });
                return false;
            }

            // TODO notify about created contest
        }
        catch (error) {
            log('warning', 'Failed to create contest object in mongodb.', { error: serializeError(error), contest: { id, tag, emoji }});
            return false;
        }
        return true;
    }

    async function list({ onlyRunning }) {
        const cursor = onlyRunning
            ? await collection.find({ running: true })
            : await collection.find();
        
        const contests = await cursor.toArray();
        return contests.map(c => ({ id: c._id, ...c}));
    }

    async function deleteContest(id) {
        try {
            const result = await collection.deleteOne({ _id: id });
            if (!result.result.ok) {
                log('info', 'Cannot delete contest: MongoDB response is not ok.', { result, id });
                return false;
            }

            if (result.deletedCount !== 1) {
                log('info', 'Response missmatch while deleting contest: deletedCount is not 1.', { result, id });
                return false;
            }

            // TODO notify about deleted contest
        }
        catch (error) {
            log('warning', 'Failed to delete contest object in mongodb.', { error: serializeError(error), contest: id});
            return false;
        }
        return true;
    }

    async function startContest(id) {
        try {
            const result = await collection.updateOne({ _id: id }, { $set: { running: true } });
            if (!result.result.ok) {
                log('info', 'Cannot start contest: MongoDB response is not ok.', { result, id });
                return false;
            }
            
            if (result.modifiedCount !== 1) {
                log('info', 'Response missmatch while starting contest: modifiedCount is not 1.', { result, id });
                return false;
            }
                                        
            // TODO notify about started contest
        }
        catch (error) {
            log('warning', 'Failed to start contest in mongodb.', { error: serializeError(error), contest: id});
            return false;
        }
        return true;
    }

    async function stopContest(id) {
        try {
            const result = await collection.updateOne({ _id: id }, { $set: { running: false } });
            if (!result.result.ok) {
                log('info', 'Cannot stop contest: MongoDB response is not ok.', { result, id });
                return false;
            }

            if (result.modifiedCount !== 1) {
                log('info', 'Response missmatch while stopping contest: modifiedCount is not 1.', { result, id });
                return false;
            }
                                        
            // TODO notify about stopped contest
        }
        catch (error) {
            log('warning', 'Failed to stop contest in mongodb.', { error: serializeError(error), contest: id});
            return false;
        }
        return true;
    }

    return { stop };
}
