/**
 * Vertebrae Inc
 * @package Cassandra-ORM
 * @exports Cassandra.Model.MaterialView
 */
"use strict";

const Cassandra = require('./cassandra');
const Model = Cassandra.Model;
const BaseModel = Model.BaseModel;

/**
 * Create a new Cassandra MaterializedView attached to a {@link Cassandra.Schema} on the named table and creating the table if it doesn't exist. In general, you should use the instanced method {@link Cassandra#createView} to attach models
 * @memberof Cassandra.Model
 * @extends Cassandra.Model
 * @param {object} model - the parent model the view will be based off of
 * @param {string} name - the materialized view's name, will become the name of the column family
 */
class MaterializedView extends Model {

    constructor(model, name, config) {
        super(model.db, name, model);
        var view = this;
        view.model = model;
        //inherit Factory from Model.ColumnFamily
        view.Factory = model.Factory;
        //expose hook methods;
        Object.defineProperties(view, {
            _find: {
                value: BaseModel.prototype.find,
                writable: false
            },
            _findOne: {
                value: BaseModel.prototype.findOne,
                writable: false
            }
        });
    }

    /**
     * Find items in the model's materialized view
     * Similar to {@link Cassandra.Model}
     * @param {object} queryObject - an object representing column:value
     * @param {array} projection - projection for selecting a subset of columns in select statements
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
    find(queryObject, projection, callback) {
        if (arguments.length === 2) {
            callback = projection;
            projection = null;
        }
        var view = this;
        if (projection) {
            view._find(queryObject, projection, callback);
        } else {
            view._find(queryObject, callback);
        }
    }

    findOne(queryObject, projection, callback) {
        if (arguments.length === 2) {
            callback = projection;
            projection = null;
        }
        var view = this;
        if (projection) {
            view._findOne(queryObject, projection, callback);
        } else {
            view._findOne(queryObject, callback);
        }
    }

}

module.exports = MaterializedView;
