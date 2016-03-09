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
 *   keyspaces: {
 *     testkeyspace: {
 *       durable_writes: true,
 *       'with replication': {
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
        if (!options || !options.keyspaces) {
            throw new Error('Cassandra connect expects options parameter to provide a keyspace property');
        }
        cassandra.keyspaces = options.keyspaces;
        cassandra.options = options;
        options.keyspaces = null;
        delete options.keyspaces;
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
                cassandra.setKeyspaces(() => {
                    cassandra.emit('connect', res);
                });
            }
        });
        return cassandra;
    }

    setKeyspaces(callback) {
        var cassandra = this;
        var keyspaces = Object.keys(cassandra.keyspaces);
        var createQuery = 'CREATE KEYSPACE IF NOT EXISTS ';
        var batch = [];
        var options, query;
        var createKeyspace = function (queryString, callback) {
                cassandra.driver.execute(queryString, callback);
            };
        for (let keyspaceName of keyspaces) {
            query = createQuery + keyspaceName;
            options = cassandra.keyspaces[keyspaceName];
            if (!!options) {
                if (options['with replication']) {
                    query += ' with replication = ' + JSON.stringify(options['with replication']).replace(/"/g, "'");
                    if (options.durable_writes) {
                        query += ' and';
                    }
                }
                if (options.durable_writes) {
                    query += ' durable_writes = ' + options.durable_writes.toString();
                }
            }
            batch.push(query);
        }
        async.eachSeries(batch, createKeyspace, (err) => {
            if (err) {
                return cassandra.emit('error', err);
            }
            callback();
        });
        
    }
    
    /**
     * Create a new {@link Cassandra.Model} attached to a {@link Cassandra.Schema}
     * object on the named table, creating the table if it doesn't exist.
     * @param {string} name - the name of the table to attach the schema to
     * @param {object} schema - the schema object
     * @example <caption>Attach a schema to a model for querying</caption>
     * var cassandra = Cassandra.connect(...);
     * //create the ORM's schema
     * var testSchema = new Cassandra.Schema(...);
     * //attach the schema to the current cassandra instance
     * var testModel = cassandra.model('test', testSchema);
     */
    model(name, schema) {
        var cassandra = this;
        var modelInstance = new Cassandra.Model(cassandra, name, schema);
        cassandra.models[modelInstance.name] = modelInstance;
        return modelInstance;
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
