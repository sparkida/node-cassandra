/**
 * Vertebrae Inc
 * @package Cassandra-ORM
 * @exports Model
 */
"use strict";

const Cassandra = require('./cassandra');
const format = require('util').format;
const async = require('async');

const OperatorMap = {
    $gt: '>',
    $gte: '>=',
    $lt: '<',
    $lte: '<=',
    $eq: '='
};

const FilterMap = {
    $in: 'IN',
    $contains: 'CONTAINS',
    $containsKey: 'CONTAINS KEY'
};

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
            throw new TypeError('Model expects parameter 1 to be an '
                + 'instanceof Cassandra');
        }
        if (!name || !name.length) {
            throw new TypeError('Model expects parameter 2 to be of type '
                + '"string": @"' + name + '"');
        }
        if (!(schema instanceof Cassandra.Schema)) {
            throw new TypeError('Model expects parameter 3 to be an '
                + 'instance of Cassandra.Schema: "' + schema + '"');
        }
        model.name = name;
        model.schema = schema;
        model.primaryKeys = schema.options.primaryKeys;
        async.series([
            (next) => model._createTable(next),
            (next) => model._createViews(next)
        ], (err) => {
            if (err) {
                db.emit('error', err);
            }
            if (typeof callback === 'function') {
                callback(err);
            }
        });
    }

    /**
     * Given a primary key array object, this will convert it to a 
     * PRIMARY KEY (...) string value to use in table creation
     * @param {array} primaryKeyArray - An array specifying the primary key columns, can be compound and composite as well
     * @private
     */
    _createPartitionKeyQuery(primaryKeyArray) {
        var query = 'PRIMARY KEY (';
        //create composite partition key
        if (Array.isArray(primaryKeyArray[0])) {
            query += format('(%s)', primaryKeyArray[0].join(', '));
            //composite key with clustering columns
            if (primaryKeyArray.length > 1) {
                query += ', ';
            }
            query += primaryKeyArray.slice(1).join(', ');
        } else {
            query += primaryKeyArray.join(', ');
        }
        query += ')';
        return query;
    }

    /**
     * Creates the table specified by the {@link Cassandra.Model} constructor
     * @param {function} callback - receives err, result
     * @private
     */
    _createTable(done) {
        var model = this;
        var schema = model.schema;
        var cassandra = model.db;
        var joinFieldTypes = (field) => {
                return field + ' ' + schema.model[field];
            };
        var query = format(
                'CREATE TABLE IF NOT EXISTS %s.%s (%s, ',
                cassandra.keyspace,
                model.name,
                Object.keys(schema.model).map(joinFieldTypes).join(', ')
            );
        query += model._createPartitionKeyQuery(model.primaryKeys);
        query += ')';
        cassandra.driver.execute(query, done);
    }

    /**
     * Creates the materialized views specified by the {@link Cassandra.Schema} options
     * @param {function} callback - receives err, result
     * @private
     */
    _createViews(done) {
        var model = this;
        var schema = model.schema;
        var views = schema.options.views;
        var cassandra = model.db;
        if (!views) {
            return done();
        }
        var batch = [];
        Object.keys(views).forEach((viewName) => {
            let view = views[viewName];
            viewName = viewName.toLowerCase();
            if (!view.primaryKeys) {
                throw new Error(format('Views require "primaryKeys" of '
                    + 'type array; model: %s, view: %s', model.name, viewName));
            }
            model.primaryKeys.forEach((key) => {
                if (view.primaryKeys.indexOf(key) === -1) {
                    view.primaryKeys.push(key);
                }
            });
            let where = view.primaryKeys.map((field) => {
                    return field + ' IS NOT NULL';
                });
            let query = format(
                    'CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s '
                    + 'AS SELECT %s FROM %s WHERE %s %s',
                    cassandra.keyspace,
                    model.name + '__' + viewName,
                    view.select 
                        ? view.select.concat(view.primaryKeys).join(', ')
                        : view.primaryKeys.join(', '),
                    model.name,
                    where.join(' AND '),
                    model._createPartitionKeyQuery(view.primaryKeys)
                );

            if (view.orderBy) {
                query += format(
                    ' WITH CLUSTERING ORDER BY (%s)',
                    Object.keys(view.orderBy).map((field) => {
                        return field + ' ' + view.orderBy[field];
                    })
                );
            }
            batch.push(query);
        });
        async.each(batch, (query, next) => cassandra.driver.execute(query, next), done);
    }

    /**
     * Checks the queryObject's properties against the qualified columns
     * @param {object} queryObject - an object representing column:value
     * @throws Error - not a valid query column
     * @private
     */
    _qualifyQueryColumns(query) {
        var model = this;
        var schema = model.schema;
        for (let field in query) {
            if (!schema.model[field]) {
                throw new Error(format('Not a valid query, could not find '
                    + 'column: %s, model: %s', field,  model.name));
            }
        }
    }

    /**
     * Insert items into the model
     * @param {object} queryObject - an object representing column:value
     * @param {function} callback - receives err, result
     */
    insert(queryObject, callback) {
        var model = this;
        var cassandra = model.db;
        var columns = Object.keys(queryObject);
        var fieldSize = columns.length;
        var values = columns.map((key) => queryObject[key]);
        var marks = new Array(fieldSize).join('?,') + '?';
        model._qualifyQueryColumns(query);
        var query = format(
                'INSERT INTO %s.%s (%s) VALUES(%s)',
                cassandra.keyspace,
                model.name,
                columns.join(', '),
                marks
            );
        cassandra.driver.execute(query, values, {prepare: true}, callback);
    }

    /**
     * Find items in the model
     * @param {object} queryObject - an object representing column:value
     * @param {array} projection - projection for selecting a subset of columns in select statements
     * @param {object} options - options for controlling your select statements
     * @param {function} callback - receives err, result
     * @example
     * var projection = ['name']; //default "*"
     * var options = {
     *   allowFiltering: true,
     *   limit: 1
     * };
     * var query = {
     *   name: 'foo',
     *   age: {
     *     $gt: 30
     *   }
     * };
     * cassandra.find(query, projection, options, (err, result) => console.log(err, result));
     * // SELECT name FROM <table> WHERE name = 'foo' AND age > 30 LIMIT 1 ALLOW FILTERING
     */
    find(queryObject, projection, options, callback) {
        var argc = arguments.length;
        if (argc === 3) {
            callback = options;
            if (!Array.isArray(projection)) {
                options = projection;
                projection = null;
            } 
        } else if (argc === 2) {
            callback = projection;
            projection = null;
        }
        var model = this;
        var cassandra = model.db;
        var columns = Object.keys(queryObject);
        var values = [];
        var where = [];
        model._qualifyQueryColumns(query);
        for (let column of columns) {
            let value = queryObject[column];
            //make sure this isn't a String or Number
            if (!value.length && isNaN(value)) {
                for (let operator in value) {
                    //$eq $gt $gte etc...
                    let mappedOperator = OperatorMap[operator];
                    if (mappedOperator) {
                        where.push(column + mappedOperator + '=?');
                        values.push(value[operator]);
                    } else {
                        //in, contains, contains key
                        mappedOperator = FilterMap[operator];
                        if (!mappedOperator) {
                            throw new Error('Invalid Operator type, not supported: ' + operator);
                        }
                        if (operator === '$in') {
                            values = values.concat(value[operator]);
                            let poly = new Array(value[operator].length);
                            where.push(column + ' ' + mappedOperator + ' (' + poly.join('?,') + '?)');
                        } else {
                            //contains, contains key
                            values.push(value[operator]);
                            where.push(column + ' ' + mappedOperator + ' ?');
                        }
                    }
                }                
            } else {
                values.push(value);
                where.push(column + '=?');
            }
        }
        
        var query = format(
                'SELECT %s FROM %s.%s WHERE %s',
                projection ? projection.join(', ') : '*',
                cassandra.keyspace,
                model.name,
                where.join(' AND ')
            );
        if (options) {
            if (options.limit) {
                query += ' LIMIT ' + options.limit;
            }
            if (options.allowFiltering) {
                query += ' ALLOW FILTERING';
            }
        }
        cassandra.driver.execute(query, values, {prepare: true}, callback);
    }

}

//TODO
//Model.toObject()

module.exports = Model;
