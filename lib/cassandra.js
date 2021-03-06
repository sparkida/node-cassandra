/**
 * Vertebrae Inc
 * @package Cassandra-ORM
 * @imports cassandra-driver
 * @exports Cassandra
 */
"use strict";

const CassandraDriver = require('cassandra-driver');
const EventEmitter = require('events');
const util = require('util');
const async = require('async');
const contactPoints = () => {
    return ['127.0.0.1'];
};
const protocolOptions = () => {
    return {
        port: 9042
    };
};

/**
 * @member {object} ConnectionOptions - documentation only
 * @property {object} keyspace - options
 * @property {array} contactPoints - a list of host IPs
 * @property {object} protocolOptions - object for port settings
 */

/**
 * Create a new Cassandra DB connection
 * @param {object} options - an object defining how the connection will be made
 * @example <caption>Connect to keyspace "testKeyspace" with options</caption>
 * var options = {
 *   contactPoints: ['127.0.0.1'],
 *   protocolOptions: {port: 9042},
 *   keyspace: {
 *     testkeyspace: {
 *       durableWrites: true,
 *       withReplication: {
 *         class: 'SimpleStrategy',
 *         replication_factor: 1
 *       }
 *     }
 *   }
 * };
 */
class Cassandra {

    constructor(options) {
        var cassandra = this;
        EventEmitter.call(this);
        cassandra.models = {};
        if (!options || !options.keyspace || Object.keys(options.keyspace).length > 1) {
            throw new Error('Cassandra connect expects options parameter to provide a single keyspace property');
        }
        for (let keyspace in options.keyspace) {
            if (!options.keyspace[keyspace].withReplication) {
                throw new Error('Cassandra connect expects "keyspace" options to provide a withReplication property');
            }
        }
        if (!options.contactPoints) {
            options.contactPoints = contactPoints();
        }
        if (!options.protocolOptions) {
            options.protocolOptions = protocolOptions();
        }
        cassandra.options = options;
        cassandra.queue = [];
        cassandra.connected = false;
        cassandra.setKeyspace();
        //clone options
        try {
            options = JSON.parse(JSON.stringify(options));
        } catch (cloneError) {
            throw new Error('Invalid options object, could not clone');
        }
        options.keyspace = undefined;
        cassandra.driver = new CassandraDriver.Client(options);
    }

    /**
     * Shortcut constructor for new Cassandra -> connect()
     * @param {object} options - an object defining how the connection will be made
     * @param {function} callback - a callback to be called once connected or error
     */
    static connect(options, callback) {
        var cassandra = new Cassandra(options);
        cassandra.connect(callback);
        return cassandra;
    }

    /**
     * Create a new connection to the database
     * @param {function} callback - a callback to be called once connected or error
     */
    connect(callback) {
        var cassandra = this,
            done = (err) => {
                if (err) {
                    if (cassandra.listenerCount('error') > 0) {
                        cassandra.emit('error', err);
                    }
                } else {
                    cassandra.connected = true;
                    if (cassandra.listenerCount('connect') > 0) {
                        cassandra.emit('connect');
                    }
                }
                if (typeof callback === 'function') {
                    callback(err);
                }
            };
        cassandra.driver.connect((err, res) => {
            if (err) {
                return done(err);
            }
            if (cassandra.queue.length) {
                async.eachSeries(cassandra.queue.splice(0), (fn, next) => {
                    //'calling: ', fn.toString()
                    fn(next);
                }, done);
            } else {
                done();
            }
        });

    }

    /**
     * Create a Uuid from the cassandra driver
     * @returns {Uuid}
     */
    uuid() {
        return Cassandra.types.Uuid.random();
    }

    /**
     * Create a TimeUuid from the cassandra driver
     * @returns {TimeUuid}
     */
    timeuuid() {
        return Cassandra.types.TimeUuid.fromDate();
    }

    /**
     * Create a TimeUuid from the cassandra driver
     * @params {int} time
     * @returns {minTimeuuid} an object to query minTimeuuid
     */
    minTimeuuid(time) {
        return new Cassandra.minTimeuuid(time);
    }

    /**
     * Create a TimeUuid from the cassandra driver
     * @params {int} time
     * @returns {minTimeuuid} an object to query maxTimeuuid
     */
    maxTimeuuid(time) {
        return new Cassandra.maxTimeuuid(time);
    }

    /**
     * Set the keyspace of the Cassandra instance (cassandra.keyspace)
     * creating it if it does not exist.
     * @param {function} callback - a callback to be called on success or error
     */
    setKeyspace(callback) {
        var cassandra = this;
        var keyspaceName = Object.keys(cassandra.options.keyspace).pop();
        var query = 'CREATE KEYSPACE IF NOT EXISTS ' + keyspaceName;
        var options = cassandra.options.keyspace[keyspaceName];
        if (!!options) {
            if (options.withReplication) {
                query += ' with replication = ' + JSON.stringify(options.withReplication).replace(/"/g, "'");
                if (options.durableWrites) {
                    query += ' and';
                }
            }
            if (options.durableWrites) {
                query += ' durable_writes = ' + options.durableWrites.toString();
            }
        }
        cassandra.keyspace = keyspaceName;
        if (cassandra.connected) {
            cassandra.driver.execute(query, callback);
        } else {
            cassandra.queue.push((next) => cassandra.driver.execute(query, next));
        }
    }

    /**
     * Create a new {@link Cassandra.Model} attached to a {@link Cassandra.Schema}
     * object on the named table, creating the table if it doesn't exist.
     * @param {string} name - the name of the table to attach the schema to
     * @param {object} schema - the schema object
     * @param {function} callback - a callback to be called once the table has been created
     * @example <caption>Attach a schema to a model for querying</caption>
     * var cassandra = Cassandra.connect(...);
     * //create the ORM's schema
     * var testSchema = new Cassandra.Schema(...);
     * //attach the schema to the current cassandra instance
     * var testModel = cassandra.model('test', testSchema);
     */
    model(name, schema, callback) {
        var cassandra = this;
        var modelInstance = new Cassandra.Model.ColumnFamily(cassandra, name, schema);
        modelInstance._buildSchema(callback);
        cassandra.models[modelInstance.name] = modelInstance;
        return modelInstance.Factory;
    }

}

module.exports = Cassandra;

util.inherits(Cassandra, EventEmitter);

class Timeuuid {
    constructor(time) {
        this.time = time;
    }
}

class MaxTimeuuid extends Timeuuid {
    get value() {
        return 'maxTimeuuid(' + this.time + ')';
    }
}
class MinTimeuuid extends Timeuuid {
    get value() {
        return 'minTimeuuid(' + this.time + ')';
    }
}
Cassandra.Timeuuid = Timeuuid;
Cassandra.minTimeuuid = MinTimeuuid;
Cassandra.maxTimeuuid = MaxTimeuuid;

/**
 * Create a Uuid from the cassandra driver
 * @function
 * @returns {Uuid}
 */
Cassandra.uuid = Cassandra.prototype.uuid;
/**
 * Create a TimeUuid from the cassandra driver
 * @function
 * @returns {TimeUuid}
 */
Cassandra.timeuuid = Cassandra.prototype.timeuuid;
/**
 * List of supported field types
 * @member
 * @see {@link https://github.com/datastax/nodejs-driver/blob/master/lib/types/index.js|CassandraDriver.types}
 */
Cassandra.types = CassandraDriver.types;
Cassandra.execute = CassandraDriver.execute;
Cassandra.utils = require('./utils');
Cassandra.Schema = require('./schema');
Cassandra.Model = require('./model');
Cassandra.Model.ColumnFamily = require('./column-family');
Cassandra.Model.ModelInstance = require('./model-instance');
Cassandra.Model.MaterializedView = require('./materialized-view');
