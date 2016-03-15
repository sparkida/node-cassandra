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
 * @param {object} columns - A list of key:values representative of field:type
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

    constructor(columns, options) {
        var schema = this;
        schema.model = {};
        schema.statics = {};
        schema.options = options || {};
        schema.columns = columns;
        if (!Array.isArray(options.primaryKeys) || options.primaryKeys.length === 0) {
            throw new Error('Schema expects have option "primaryKeys" of type array');
        }
        schema._prepareColumns();
        schema._qualifyPrimaryKeys();
        if (options.views) {
            schema._qualifyViews();
        }
    }
    
    /**
     * Prepare Schema columns and validate their types
     * This will check the list of types against the {@link Cassandra.types}
     * @see {@link https://github.com/datastax/nodejs-driver/blob/master/lib/types/index.js|CassandraDriver.types}
     */
    _prepareColumns() {
        var schema = this;
        var columns = schema.columns;
        schema.columns = [];
        for (let field in columns) {
            //TODO check if object with options
            let type = columns[field].toLowerCase();
            if (!dataTypes[type]) {
                throw new TypeError('Cassandra data type not supported: "' + type + '"');
            }
            schema.model[field] = type;
            schema.columns.push(field);
        }
    }

    /**
     * Validates each view defined by the schema's options
     * @param {object} views - an object of defined views
     * @throws Error - invalid view settings
     * @returns {undefined}
     */
    _qualifyViews(views) {
        var schema = this;
        views = views || schema.options.views;
        for (let viewName in views) {
            schema.qualifyView(viewName, views[viewName]);
        }
    }

    /**
     * Qualify a single view by name
     * @param {string} viewName - the name of the materialized view, this will be convert to <columnFamilyName>___<viewName>
     * @param {object} viewConfig - a valid view configuration
     * @returns {boolean} true - if successful
     * @throws Error - invalid view settings
     */
    qualifyView(viewName, viewConfig) {
        var schema = this;
        var model = schema.model;
        var view = viewConfig;
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
        return true;
    }

    /**
     * Validates keys against the prepared model
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
