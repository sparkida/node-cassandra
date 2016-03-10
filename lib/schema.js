/**
 * Vertebrae Inc
 * @package Cassandra-ORM
 * @imports cassandra-driver
 * @exports Schema
 */
"use strict";

const types = require('./cassandra').types;
const dataTypes = types.dataTypes;
const format = require('util').format;

/**
 * Create a new Cassandra Schema to be attached to the models
 * @memberof Cassandra
 * @param {object} fields - A list of key:values representative of field:type
 * @param {object} options - A list of schema specific options, primary/composite keys
 * @example <caption>Create a new Schema Object</caption>
 * var testSchema = new Cassandra.Schema({
 *   name: 'text',
 *   age: 'int',
 *   username: 'text'
 * }, {
 *   primaryKeys: ['username'],
 *   views: {
 *     byName: {
 *       primaryKeys: ['name'],//, 'username'], is implied
 *       orderBy: {
 *         name: 'desc',
 *         username: 'asc'
 *       }
 *     }
 *   }
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
        schema._prepareFields();
        schema._qualifyPrimaryKeys();
        if (options.views) {
            schema._qualifyViews();
        }
    }
    
    /**
     * Prepare Schema fields and validate their types
     * This will check the list of types against the {@link Cassandra.types}
     * @see {@link https://github.com/datastax/nodejs-driver/blob/master/lib/types/index.js|CassandraDriver.types}
     * @private
     * @param {object} fields - a key:value of field:type (optional)
     */
    _prepareFields(fields) {
        var schema = this;
        fields = fields || schema.fields;
        for (let field in fields) {
            //TODO check if object with options
            let type = fields[field].toLowerCase();
            if (!dataTypes[type]) {
                throw new TypeError('Cassandra data type not supported: "' + type + '"');
            }
            schema.model[field] = type;
        }
    }

    /**
     * Validates each view defined by the schema's options
     * @private
     * @param {object} views - an object of defined views
     */
    _qualifyViews(views) {
        var schema = this;
        var model = schema.model;
        views = views || schema.options.views;
        for (let viewName in views) {
            let view = views[viewName];
            if (!view.select && !view.primaryKeys) {
                throw new Error(format('Views must specify at least a "primaryKeys" '
                    + 'property to create from table; view: %s', viewName));
            }
            if (view.select) {
                for (let column of view.select) {
                    if (!model[column]) {
                        throw new Error(format(
                            'Could not add materialized view, undefined '
                            + 'column in select array; view: %s, column: %s', 
                            viewName,
                            column
                        ));
                    }
                }
            }
            if (view.primaryKeys) {
                for (let column of view.primaryKeys) {
                    if (!model[column]) {
                        throw new Error(format(
                            'Could not add materialized view, undefined '
                            + 'column in primaryKeys array; view: %s, column: %s', 
                            viewName,
                            column
                        ));
                    }
                }
            }
            if (view.orderBy) {
                for (let column in view.orderBy) {
                    if (!model[column]) {
                        throw new Error(format(
                            'Could not add materialized view, undefined '
                            + 'column in primaryKeys array; view: %s, column: %s', 
                            viewName,
                            column
                        ));
                    }
                }
            }
        }
    }

    /**
     * Validates keys against the prepared model
     * @private
     * @param {array} primaryKeys - an array of primary keys
     */
    _qualifyPrimaryKeys(primaryKeys) {
        var schema = this;
        var model = schema.model;
        primaryKeys = primaryKeys || schema.options.primaryKeys;
        //composite
        if (Array.isArray(primaryKeys[0])) {
            primaryKeys = primaryKeys[0].concat(primaryKeys.slice(1));
        }
        for (let key of primaryKeys) {
            if (!model[key]) {
                throw new Error('Invalid Primary Key, column not '
                    + 'found in schema model; @key: ' + key);
            }
        }
    }
}

module.exports = Schema;
