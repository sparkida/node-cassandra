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

console.log('rolling');

class Cassandra {
    
    constructor(options) {
        var cassandra = this;
        EventEmitter.call(this);
        cassandra.models = {};
        cassandra.keyspace = options.keyspace;
        if (keyspace) {
            options.keyspace = null;
        }
        cassandra.driver = new CassandraDriver.Client(options);
    }

    static connect(options) {
        var cassandra = new Cassandra(options);
        cassandra.driver.connect((err, res) => {
            if (err) {
                cassandra.emit('error', err);
            } else {
                if (cassandra.keyspace) {
                    cassandra.setKeyspaces();
                }
                cassandra.emit('connect', res);
            }
        });
        return cassandra;
    }

    model(name, schema) {
        var cassandra = this;
        var modelInstance = new Cassandra.Model(cassandra, name, schema);
        cassandra.models[modelInstance.name] = modelInstance;
        console.log('models:', cassandra.models);
        return modelInstance;
    }

}

module.exports = Cassandra;

util.inherits(Cassandra, EventEmitter);

Cassandra.types = CassandraDriver.types;
Cassandra.execute = CassandraDriver.execute;
Cassandra.Schema = require('./schema');
Cassandra.Model = require('./model');

console.log('loaded!', Cassandra.connect);
