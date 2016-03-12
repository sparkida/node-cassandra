/**
 * Vertebrae Inc
 * @package Cassandra-ORM
 * @exports MaterialView
 */
"use strict";

const Cassandra = require('./cassandra');
const Model = Cassandra.Model;
const format = require('util').format;
const async = require('async');
const BaseModel = Model.BaseModel;
const AbstractModel = Model.AbstractModel;
const AbstractModelPrototype = AbstractModel.prototype;
const abstractMethods = Object.getOwnPropertyNames(AbstractModelPrototype);

/**
 * Create a new Cassandra ColumnFamily attached to a {@link Cassandra.Schema} on the named table and creating the table if it doesn't exist. In general, you should use the instanced method {@link Cassandra#model} to attach models
 * @memberof Cassandra.Model
 * @param {object} model - the parent model the view will be based off of
 * @param {string} name - the materialized view's name, will become the name of the column family
 */
class ColumnFamily extends BaseModel {

    constructor(db, name, schema) {
        super(db, name, schema);
        var model = this;
        //instance Factory
        model.Factory = class extends Model.ModelInstance {
            constructor(object) {
                super(object, model);
            }
            static get name() {
                return model.name;
            }
            static get views() {
                return model.views;
            }
            static get model() {
                return model;
            }
        };
        //mixin abstract model
        for (let method of abstractMethods) {
            if (method === 'constructor') {
                continue;
            }
            //expose ORM specific methods
            Object.defineProperty(model.Factory, method, {
                enumerable: true,
                value: function () {
                    return AbstractModelPrototype[method].apply(model, arguments);
                }
            });
        }
        //mixin statics in both instance and class
        for (let method in schema.statics) {
            let staticMethod = schema.statics[method];
            model[method] = staticMethod;
            model.Factory[method] = function () {
                staticMethod.apply(model, arguments);
            };
        }
    }

    _buildSchema(callback) {
        var model = this;
        var db = model.db;
        if (model.__$$built) {
            throw new Error('This should not be overriden!');
        }
        model.__$$built = true;
        model.views = {};
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
     * Creates the table specified by the {@link Cassandra.Model} constructor
     * @param {function} callback - receives err, result
     */
    _createTable(done) {
        var model = this;
        var schema = model.schema;
        var cassandra = model.db;
        var joinColumnTypes = (column) => {
                return column + ' ' + schema.model[column];
            };
        var query = format(
                'CREATE TABLE IF NOT EXISTS %s.%s (%s, ',
                cassandra.keyspace,
                model.name,
                schema.columns.map(joinColumnTypes).join(', ')
            );
        query += model._createPartitionKeyQuery(model.primaryKeys);
        query += ')';
        cassandra.driver.execute(query, done);
    }

    /**
     * Creates the materialized views specified by the {@link Cassandra.Schema} options
     * @param {function} callback - receives err, result
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
        for (let viewName in views) {
            batch.push(model.createView(viewName, views[viewName], false, true));
        }
        async.each(batch, (query, next) => cassandra.driver.execute(query, next), done);
    }

    /**
     * Creates a new materialized view and attachs it to the model {@link Cassandra.Model#views}
     * @param {string} viewName - the name of the current view
     * @param {object} config - the views configuration object
     * @param {function|boolean} callback|querystring - if callback, receieves err, result. Else if "false" will return querystring
     * @param {boolean} noQualify - do not attempt to qualify the configuration
     */
    createView(viewName, config, done, noQualify) {
        var view = config;
        var model = this;
        var cassandra = model.db;
        if (model.views[viewName]) {
            throw new Error('View already exists! ' + model.name + ' - ' + viewName);
        }
        model.views[viewName] = new Model.MaterializedView(model, viewName, config);
        if (!noQualify) {
            model.schema.qualifyView(viewName, config);
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
                model.views[viewName].qualifiedName,
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
        if (!done) {
            return query;
        } else {
            cassandra.driver.execute(query, done);
        }
    }

}

module.exports = ColumnFamily;
