/**
 * Vertebrae Inc
 * @package Cassandra-ORM
 * @imports cassandra-driver
 * @exports Schema
 */
"use strict";

const types = require('./cassandra').types;
const dataTypes = types.dataTypes;

class Schema {
    constructor(fields, options) {
        var schema = this;
        schema.model = {};
        schema.options = options || {};
        schema.fields = fields;
        schema.prepareFields();
    }
    
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
