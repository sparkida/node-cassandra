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
const arrayUnique = (val, index, self) => {
    return self.indexOf(val) === index;
};

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
        model.views = {};
        model.indexes = {};
        //instance Factory
        model.Factory = class extends Model.ModelInstance {
            constructor(object, bypass) {
                super(object, model, bypass);
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
        var cassandra = model.db;
        if (model.__$$built) {
            throw new Error('This should not be overriden!');
        }
        model.__$$built = true;
        if (cassandra.connected) {
            async.series([
                (next) => model._createTable(next),
                (next) => model._createViews(next),
                (next) => model._createIndexes(next)
            ], (err) => {
                if (typeof callback === 'function') {
                    callback(err);
                }
            });
        } else {
            model._createTable();
            model._createViews();
            model._createIndexes();
        }
    }

    /**
     * Creates the table specified by the {@link Cassandra.Model} constructor
     * @param {function} callback - receives err, result
     */
    _createTable(done) {
        var model = this;
        var schema = model.schema;
        var dataModel = schema.model;
        var collections = schema.collections;
        var compaction = schema.options.compaction;
        var cassandra = model.db;
        var orderBy = schema.options.orderBy;
        var options = [];
        var joinColumnTypes = (column) => {
                if (collections[column]) {
                    let collection = collections[column];
                    return format(
                        '%s %s<%s>',
                        column,
                        schema.model[column],
                        dataModel[column] === 'map' ? collection.join(', ') : collection
                    );
                }
                return column + ' ' + schema.model[column];
            };
        var query = format(
                'CREATE TABLE IF NOT EXISTS %s.%s (%s, %s)',
                cassandra.keyspace,
                model.name,
                schema.columns.map(joinColumnTypes).join(', '),
                model._createPartitionKeyQuery(model.primaryKeys)
            );
        if (orderBy) {
            options.push(format(
                'CLUSTERING ORDER BY (%s)',
                Object.keys(orderBy).map((field) => {
                    return field + ' ' + orderBy[field];
                })
            ));
        }
        if (compaction) {
            options.push('compaction = ' + JSON.stringify(compaction).replace(/"/g, "'"));
        }
        if (options.length) {
            query += ' WITH ' + options.join(' AND ');
        }
        if (cassandra.connected) {
            cassandra.driver.execute(query, done);
        } else {
            cassandra.queue.push((next) => cassandra.driver.execute(query, next));
        }
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
            if (typeof done === 'function') {
                done();
            }
            return;
        }
        var batch = [];
        for (let viewName in views) {
            batch.push(model.createView(viewName, views[viewName], false, true));
        }
        if (cassandra.connected) {
            async.each(batch, (query, next) => cassandra.driver.execute(query, next), done);
        } else {
            cassandra.queue.push((next) => {
                async.each(batch, (query, cb) => cassandra.driver.execute(query, cb), next);
            });
        }
    }

    _createIndexes(done) {
        var model = this;
        var indexes = model.schema.options.indexes;
        var cassandra = model.db;
        if (!indexes || !indexes.length) {
            if (typeof done === 'function') {
                done();
            }
            return;
        }
        var batch = [];
        for (let index of indexes) {
            batch.push(model.createIndex(index, false, true));
        }
        if (cassandra.connected) {
            async.each(batch, (query, next) => cassandra.driver.execute(query, next), done);
        } else {
            cassandra.queue.push((next) => {
                async.each(batch, (query, cb) => cassandra.driver.execute(query, cb), next);
            });
        }
    }

    /**
     * Create an Index on the current table
     * @param {string|object} index - a string of the index name that will be automatically mapped, or an object with a single key as the index who's value is the custom index name to be stored in the database.
     * @param {function} done - receives err, result
     * @example
     * createIndex('myIndex', ...); // translates to <table>_myindex_idx
     * createIndex({myIndex: 'custom_index_mapping'}, ...); // does not get translated
     */
    createIndex(index, done) {
        var model = this;
        var query = '';
        var cassandra = model.db;
        var dataModel = model.schema.model;
        var keyspace = cassandra.keyspace;
        //string
        if (index.length) {
            if (undefined === dataModel[index]) {
                throw new Error(format('Invalid Index, could not find column "%s" in table model "%s"', index, model.name));
            }
            model.indexes[index] = index;
            query = format(
                'CREATE INDEX IF NOT EXISTS %s_%s_idx ON %s.%s (%s)',
                model.name,
                index,
                keyspace,
                model.name,
                dataModel[index] === 'map' ? format('KEYS(%s)', index) : index
            );
        } else {
            let indexKey = Object.keys(index)[0];
            let indexName = index[indexKey];
            if (undefined === dataModel[indexKey]) {
                throw new Error(format('Invalid Index, could not find column "%s" in table model "%s"', indexKey, model.name));
            }
            model.indexes[index] = index;
            model.indexes[indexKey] = indexName;
            query = format(
                'CREATE INDEX IF NOT EXISTS %s ON %s.%s (%s)',
                indexName,
                keyspace,
                model.name,
                dataModel[indexKey] === 'map' ? format('KEYS(%s)', indexKey) : indexKey
            );
        }
        if (false === done) {
            return query;
        } else if (cassandra.connected) {
            cassandra.driver.execute(query, done);
        } else {
            cassandra.queue.push((next) => cassandra.driver.execute(query, next));
        }
    }

    /**
     * Creates a new materialized view and attachs it to the model {@link Cassandra.Model#views}
     * The create query that is generated will mixin any missing primary keys from the model,
     * this means if your model has a composite key [a,b] and your view sets a composite key of
     * [[c,d],e
     * @param {string} viewName - the name of the current view
     * @param {object} config - the views configuration object
     * @param {function|boolean} callback|querystring - if callback, receieves err, result. Else if "false" will return querystring, this is strict
     * @param {boolean} noQualify - do not attempt to qualify the configuration
     */
    createView(viewName, config, done, noQualify) {
        var view = config;
        var model = this;
        var cassandra = model.db;
        if (model.views[viewName]) {
            let error = new Error('View already exists! ' + model.name + ' - ' + viewName);
            if (typeof done === 'function') {
                return done(error);
            } else {
                throw error;
            }
        }
        model.views[viewName] = new Model.MaterializedView(model, viewName, config);
        if (!noQualify) {
            model.schema.qualifyView(viewName, config);
        }
        //Mixin model's primary keys
        var primaryKeys = Array.isArray(view.primaryKeys[0]) 
            ? view.primaryKeys[0].concat(view.primaryKeys.slice(1)) 
            : view.primaryKeys.slice(0);
        var modelPrimaryKeys = Array.isArray(model.primaryKeys[0]) 
            ? model.primaryKeys[0].concat(model.primaryKeys.slice(1)) 
            : model.primaryKeys.slice(0);
        modelPrimaryKeys.forEach((key) => {
            if (primaryKeys.indexOf(key) === -1) {
                view.primaryKeys.push(key);
                primaryKeys.push(key);
            }
        });
        let where = primaryKeys.map((column) => {
                return column + ' IS NOT NULL';
            });
        let query = format(
                'CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s '
                + 'AS SELECT %s FROM %s WHERE %s %s',
                cassandra.keyspace,
                model.views[viewName].qualifiedName,
                view.select
                    ? view.select.concat(view.primaryKeys).filter(arrayUnique).join(', ')
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
        if (false === done) {
            return query;
        } else if (cassandra.connected) {
            cassandra.driver.execute(query, done);
        } else {
            cassandra.queue.push((next) => cassandra.driver.execute(query, next));
        }
    }

}

module.exports = ColumnFamily;
