/**
 * Vertebrae Inc
 * @package Cassandra-ORM
 * @exports Model
 */
"use strict";

const Cassandra = require('./cassandra');
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

        }
        model.name = name;
        model.schema = schema;
        model.createKeyspaces();
    }

    createKeyspaces() {
        var query = 'CREATE KEYSPACE IF NOT EXISTS';


    }
}

module.exports = Model;
