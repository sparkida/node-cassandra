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
        cassandra.keyspace = options.keyspace;
        cassandra.options = options;
        options.keyspace = null;
        delete options.keyspace;
        cassandra.driver = new CassandraDriver.Client(options);
    }

    /**
     * Shortcut constructor for Cassandra
     * @param {object} options - an object defining how the connection will be made
     */
    static connect(options) {
        var cassandra = new Cassandra(options);
        cassandra.driver.connect((err, res) => {
            if (err) {
                cassandra.emit('error', err);
            } else {
                cassandra.setKeyspace(() => {
                    cassandra.emit('connect', res);
                });
            }
        });
        return cassandra;
    }

    /**
     * Create a Uuid from the cassandra driver
     * @returns {Uuid}
     */
    static uuid() {
        return Cassandra.types.Uuid.random();
    }
    
    /**
     * Create a TimeUuid from the cassandra driver
     * @returns {TimeUuid}
     */
    static timeuuid() {
        return Cassandra.types.TimeUuid.fromDate();
    }

    setKeyspace(callback) {
        var cassandra = this;
        var keyspaceName = Object.keys(cassandra.keyspace).pop();
        var query = 'CREATE KEYSPACE IF NOT EXISTS ' + keyspaceName;
        var options = cassandra.keyspace[keyspaceName];
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
        cassandra.keyspace = keyspaceName.toLowerCase();
        cassandra.driver.execute(query, callback);
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

/** 
 * List of supported field types
 * @member
 * @see {@link https://github.com/datastax/nodejs-driver/blob/master/lib/types/index.js|CassandraDriver.types}
 */
Cassandra.types = CassandraDriver.types;
Cassandra.execute = CassandraDriver.execute;
Cassandra.Schema = require('./schema');
Cassandra.Model = require('./model');
Cassandra.Model.ColumnFamily = require('./column-family');
Cassandra.Model.ModelInstance = require('./model-instance');
Cassandra.Model.MaterializedView = require('./materialized-view');
