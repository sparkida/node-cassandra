/**
 * Vertebrae Inc
 * @package Cassandra-ORM
 * @imports cassandra-driver
 * @exports Schema
 */
"use strict";

const types = require('./cassandra').types;
const dataTypes = types.dataTypes;

/**
 * Create a new Cassandra Schema to be attached to the models
 * @memberof Cassandra
 * @param {object} fields - A list of key:values representative of field:type
 * @param {object} options - A list of schema specific options, primary/composite keys
 * @example <caption>Create a new Schema Object</caption>
 * var testSchema = new Cassandra.Schema({
 *   name: 'text',
 *   age: 'int'
 * }, {
 *   primaryKeys: ['name']
 * });
 */
class Schema {
    constructor(fields, options) {
        var schema = this;
        schema.model = {};
        schema.options = options || {};
        schema.fields = fields;
        if (!Array.isArray(options.primaryKeys) || options.primaryKeys.length === 0) {
            throw new Error('Schema expects have option "primaryKeys" of type array');
        }
        schema.prepareFields();
    }
    
    /**
     * Prepare Schema fields and validate their types
     * This will check the list of types against the {@link Cassandra.types}
     * @see {@link https://github.com/datastax/nodejs-driver/blob/master/lib/types/index.js|CassandraDriver.types}
     */
    prepareFields(fields) {
        var schema = this;
        var field;
        fields = fields || schema.fields;
        for (field in fields) {
            //TODO check if object with options
            let type = fields[field].toLowerCase();
            if (!dataTypes[type]) {
                throw new TypeError('Cassandra data type not supported: "' + type + '"');
            }
            schema.model[field] = type;
        }
    }
}

module.exports = Schema;
