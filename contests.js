const { log } = require('./log');
const { serializeError } = require('serialize-error');
const { Worker, Publisher } = require('redis-request-broker');
const Redis = require("ioredis");
const { MongoClient } = require('mongodb');

module.exports.build = async function build(config) {

    const mongoClient = new MongoClient(config.mongodb.connectionString, { useNewUrlParser: true, useUnifiedTopology: true });
    const workers = {};
    const publishers = {};
    /** @type {import('mongodb').Collection} */
    let collectionContests;
    let collectionMemes;

    try {
        await start()
    }
    catch (error) {
        await log('error', 'Failed to start contests.', serializeError(error));
        await stop();
        throw error;
    }

    async function start() {
        try {
            // Connect to mongodb
            await mongoClient.connect();
            const db = mongoClient.db(config.mongodb.database);
            collectionContests = await db.createCollection(config.mongodb.collectionContests);
            collectionMemes = await db.createCollection(config.mongodb.collectionMemes);

            // Start worker
            workers.create = new Worker(config.rrb.queues.contestsCreate, createContest);
            workers.list = new Worker(config.rrb.queues.contestsList, list);
            workers.delete = new Worker(config.rrb.queues.contestsDelete, deleteContest);
            workers.start = new Worker(config.rrb.queues.contestsStart, startContest);
            workers.stop = new Worker(config.rrb.queues.contestsStop, stopContest);
            workers.top = new Worker(config.rrb.queues.contestsTop, contestGetTop);
            publishers.started = new Publisher(config.rrb.channels.contestStarted);
            publishers.stopped = new Publisher(config.rrb.channels.contestStopped);
            publishers.created = new Publisher(config.rrb.channels.contestCreated);
            publishers.deleted = new Publisher(config.rrb.channels.contestDeleted);
            await workers.create.listen();
            await workers.list.listen();
            await workers.delete.listen();
            await workers.start.listen();
            await workers.stop.listen();
            await workers.top.listen();
            await publishers.started.connect();
            await publishers.stopped.connect();
            await publishers.created.connect();
            await publishers.deleted.connect();
        }
        catch (error) {
            await log('error', 'Failed to init contests', serializeError(error));
            await stop();
            throw error;
        }
    }

    async function stop() {
        try {
            // Stop worker
            for (const w of Object.values(workers)) {
                await w.stop().catch(e => log.log('warning', 'Cannot stop worker', serializeError(e)));
            }

            for (const p of Object.values(publishers)) {
                await p.disconnect().catch(e => log.log('warning', 'Cannot stop publisher', serializeError(e)));
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
            const result = await collectionContests.insertOne({_id: id, tag, emoji, running: false});
            if (!result.result.ok) {
                log('info', 'Cannot create contest: MongoDB response is not ok.', { result, id });
                return false;
            }
            
            if (result.insertedCount !== 1) {
                log('info', 'Response missmatch while creating contest: insertedCount is not 1.', { result, id });
                return false;
            }

            await publishers.created.publish({ id, tag, emoji, running: false });
        }
        catch (error) {
            log('warning', 'Failed to create contest object in mongodb.', { error: serializeError(error), contest: { id, tag, emoji }});
            return false;
        }
        return true;
    }

    async function list({ onlyRunning }) {
        const cursor = onlyRunning
            ? await collectionContests.find({ running: true })
            : await collectionContests.find();
        
        const contests = await cursor.toArray();
        return contests.map(c => ({ id: c._id, ...c}));
    }

    async function deleteContest(id) {
        try {
            const result = await collectionContests.deleteOne({ _id: id });
            if (!result.result.ok) {
                log('info', 'Cannot delete contest: MongoDB response is not ok.', { result, id });
                return false;
            }

            if (result.deletedCount !== 1) {
                log('info', 'Response missmatch while deleting contest: deletedCount is not 1.', { result, id });
                return false;
            }

            await publishers.deleted.publish(id);
        }
        catch (error) {
            log('warning', 'Failed to delete contest object in mongodb.', { error: serializeError(error), contest: id});
            return false;
        }
        return true;
    }

    async function startContest(id) {
        try {
            const result = await collectionContests.updateOne({ _id: id }, { $set: { running: true } });
            if (!result.result.ok) {
                log('info', 'Cannot start contest: MongoDB response is not ok.', { result, id });
                return false;
            }
            
            if (result.modifiedCount !== 1) {
                log('info', 'Response missmatch while starting contest: modifiedCount is not 1.', { result, id });
                return false;
            }
            
            await publishers.started.publish(id);
        }
        catch (error) {
            log('warning', 'Failed to start contest in mongodb.', { error: serializeError(error), contest: id});
            return false;
        }
        return true;
    }

    async function stopContest(id) {
        try {
            const result = await collectionContests.updateOne({ _id: id }, { $set: { running: false } });
            if (!result.result.ok) {
                log('info', 'Cannot stop contest: MongoDB response is not ok.', { result, id });
                return false;
            }

            if (result.modifiedCount !== 1) {
                log('info', 'Response missmatch while stopping contest: modifiedCount is not 1.', { result, id });
                return false;
            }

            await publishers.started.publish(id);
        }
        catch (error) {
            log('warning', 'Failed to stop contest in mongodb.', { error: serializeError(error), contest: id});
            return false;
        }
        return true;
    }

    async function contestGetTop({ id, vote_type, amount }) {
        const contest = await collectionContests.findOne({ _id: id });

        if (!contest._id)
            throw new Error('Contest does not exist');
        
        const cursor = await collectionMemes.find({ contests: contest.tag }, { 
            sort: {
                [`votes.${vote_type}`]: -1
            },
            limit: amount,
            projection: {
                _id: 1
            }
         });
        
        const memes = await cursor.toArray();
        return memes.map(m => m._id);
    }

    return { stop };
}
