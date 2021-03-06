/**
 * Vertebrae Inc
 * @package Cassandra-ORM
 * @exports Cassandra.Model.MaterialView
 */
"use strict";

const Cassandra = require('./cassandra');
const Model = Cassandra.Model;
const BaseModel = Model.BaseModel;
const getFilteredArgs = Cassandra.utils.getFilteredArgs;

/**
 * Create a new Cassandra MaterializedView attached to a {@link Cassandra.Schema} on the named table and creating the table if it doesn't exist. In general, you should use the instanced method {@link Cassandra.Model.ColumnFamily#createView|Model.createView()} to attach models
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
     * Similar to {@link Cassandra.AbstractModel#find}
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
    find() {
        var args = getFilteredArgs(arguments);
        var view = this;
        view._find.apply(view, args);
    }

    /**
     * Find a single row in the materialized view
     * Similar to {@link Cassandra.AbstractModel#findOne}
     */
    findOne() {
        var args = getFilteredArgs(arguments);
        var view = this;
        view._findOne.apply(view, args);
    }

}

module.exports = MaterializedView;
