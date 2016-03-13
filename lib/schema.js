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
 *   id: {default: Cassandra.uuid, required: true},
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
        options = options || {};
        var schema = this;
        schema.model = {};
        schema.statics = {};
        schema.options = options;
        schema.required = {}; //list of required columns
        schema.defaults = {}; //list of
        schema.columns = []; //list of
        if (!options.primaryKeys || !Array.isArray(options.primaryKeys) || options.primaryKeys.length === 0) {
            throw new Error('Schema expects have option "primaryKeys" of type array');
        }
        schema._prepareColumns(columns);
        schema._qualifyPrimaryKeys();
        if (options.views) {
            schema._qualifyViews();
        }
    }

    /**
     * Prepare Schema columns and validate their types this will set the schema's
     * columns array list and set the "default", "required", and "type" settings
     * for the columns, populating {@link Cassandra.Schema.columns|schema.columns},
     * {@link Cassandra.Schema.required}, {@link Cassandra.Schema.defaults}.
     * This will check the list of types against the {@link Cassandra.types.dataTypes}
     * @param {object} columns - A list of key:values representative of field:type
     * @see {@link https://github.com/datastax/nodejs-driver/blob/master/lib/types/index.js|CassandraDriver.types}
     */
    _prepareColumns(columns) {
        if (!columns) {
            throw new Error('Must provide an object structure of your schema');
        }
        var schema = this,
            column,
            type;
        for (let field in columns) {
            column = columns[field];
            type = column.type || column;
            //if type is object instead of string, then get the type property
            if (!dataTypes[type]) {
                throw new TypeError('Cassandra data type not supported: "' + type + '"');
            }
            schema.model[field] = type;
            if (column.required) {
                schema.required[field] = 1;
            }
            if (column.default) {
                schema.defaults[field] = column.default;
            }
            schema.columns.push(field);
        }
        schema.columns = schema.columns.sort();
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
