/**
 * Vertebrae Inc
 * @package Cassandra-ORM
 * @exports Model
 */
"use strict";

const Cassandra = require('./cassandra');
const format = require('util').format;

/**
 * Create a new Cassandra Model attached to a {@link Cassandra.Schema} on the named table and creating the table if it doesn't exist. In general, you should use the instanced method {@link Cassandra#model} to attach models
 * @memberof Cassandra
 * @param {object} db - the database connection instance
 * @param {string} name - the model name, will become the name of the table
 * @param {object} schema - the {@link Cassandra.Schema} to be attached
 * @param {function} callback - a callback to be called once the table has been created
 * @example <caption>Attach a schema to a model for querying</caption>
 * var cassandra = Cassandra.connect(...);
 * var testSchema = new Cassandra.Schema(...);
 * var testModel = new Cassandra.Model(cassandra, 'test', testSchema);
 */
class Model {

    constructor(db, name, schema, callback) {
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
        model.name = name;
        model.schema = schema;
        model.createTable(callback);
    }

    /**
     * Creates the table specified by the {@link Cassandra.Model} constructor
     */
    createTable(done) {
        var model = this;
        var schema = model.schema;
        var cassandra = model.db;
        var joinFieldTypes = (field) => {
                return field + ' ' + schema.model[field];
            };
        var query = format(
                'CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY (',
                cassandra.keyspace,
                model.name,
                Object.keys(schema.model).map(joinFieldTypes).join(', ')
            );
        //create composite partition key
        if (Array.isArray(schema.options.primaryKeys[0])) {
            query += format('(%s)', schema.options.primaryKeys[0].join(', '));
            //composite key with clustering columns
            if (schema.options.primaryKeys.length > 1) {
                query += ', ';
            }
            query += schema.options.primaryKeys.slice(1).join(', ');
        } else {
            query += schema.options.primaryKeys.join(', ');
        }
        query += '))';
        cassandra.driver.execute(query, (err, result) => {
            if (err) {
                cassandra.emit('error', err);
            }
            if (typeof done === 'function') {
                done(err, result);
            }
        });
    }
}

module.exports = Model;
