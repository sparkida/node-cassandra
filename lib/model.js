/**
 * Vertebrae Inc
 * @package Cassandra-ORM
 * @exports Model
 */
"use strict";

const Cassandra = require('./cassandra');

/**
 * Create a new Cassandra Model attached to a {@link Cassandra.Schema} on the named table and creating the table if it doesn't exist. In general, you should use the instanced method {@link Cassandra#model} to attach models
 * @memberof Cassandra
 * @param {object} db - the database connection instance
 * @param {string} name - the model name, will become the name of the table
 * @param {object} schema - the {@link Cassandra.Schema} to be attached
 * @example <caption>Attach a schema to a model for querying</caption>
 * var cassandra = Cassandra.connect(...);
 * var testSchema = new Cassandra.Schema(...);
 * var testModel = new Cassandra.Model(cassandra, 'test', testSchema);
 */
class Model {
    constructor(db, name, schema) {
        var model = this;
        model.db = db;
        if (!db instanceof Cassandra) {
            throw new TypeError('Model expects parameter 1 to be an instanceof Cassandra');
        }
        if (!name || !name.length) {
            throw new TypeError('Model expects parameter 2 to be of type "string": @"' + name + '"');
        }
        if (!(schema instanceof Cassandra.Schema)) {
            throw new TypeError('Model expects parameter 3 to be an instance of Cassandra.Schema: "' + schema + '"');
        }
        if (!Array.isArray(schema.options.primaryKeys) || schema.options.primaryKeys.length === 0) {
            throw new Error('Model expects schema to have options.primaryKeys array');
        }
        model.name = name;
        model.schema = schema;
        model.createTable();
    }

    createTable() {



    }
}

module.exports = Model;
