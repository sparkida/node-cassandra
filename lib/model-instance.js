/**
 * Vertebrae Inc
 * @package Cassandra-ORM
 * @exports Model
 */
"use strict";

const format = require('util').format;

class ModelInstance {

    constructor(object, model) {
        var instance = this;
        //keep these hidden from enumerators
        Object.defineProperties(instance, {
            object: {
                value: object,
                writable: false
            },
            model: {
                value: model,
                writable: false
            },
            views: {
                value: model.views,
                writable: false
            },
            __$$synced: {
                value: false,
                writable: true,
                enumerable: false
            }
        });

        instance._prepareObject();
    }

    _prepareObject() {
        var instance = this,
            save = {},
            object = instance.object,
            model = instance.model,
            schema = model.schema,
            columns = schema.columns,
            required = schema.required,
            defaults = schema.defaults;
        for (let column of columns) {
            let defaultVal = defaults[column];
            let objectValue = object[column];
            if (undefined === objectValue) {
                //if a default is set and no value is provided
                if (undefined !== defaultVal) {
                    //if it has a apply property
                    if (typeof defaultVal === 'function') {
                        save[column] = defaultVal();
                    } else {
                        save[column] = defaultVal;
                    }
                } else if (required[column]) {
                    throw new Error(format(
                        'Column value is required > column: %s, columnFamily: %s',
                        column,
                        model.name
                    ));
                }
            } else {
                save[column] = objectValue;
            }
            instance[column] = save[column];
        }
        Object.defineProperty(instance, '__$$object', {
            value: save,
            writable: false
        });
    }

    /**
     * Instanced Models have the ability to rollback failed
     * writes, for example, if you attempt to "model.save(...)"
     * and this produces an errror, in your callback you can
     * then determine if you wish to restore the object data
     * model back to the values it has saved from the last sync
     * with your database or continue on with the local data
     * @example <caption>Restore last data synced</caption>
     * //user schema: { name: 'text' }, { primaryKeys: [ 'name' ] }
     * user = new TestModel({name: 'foo'});
     * //we should not be able to update a primary key
     * user.name = 'bar';
     * user.save((err) => {
     *   if (err) {
     *     //user.name == 'bar'
     *     user.restore();
     *     //user.name == 'foo'
     *   }
     * });
     */
    restore() {
        var instance = this;
        var data = instance.__$$object;
        for (let column in data) {
            instance[column] = data[column];
        }
    }

    /**
     * Save the current instance's data model to the table
     * this will create a new row if you have altered a primary key
     * @param {function} callback - receives err
     */
    sync(callback) {
        var instance = this;
        var model = instance.model;
        var schema = model.schema;
        var data = instance.__$$object;
        var columns = schema.columns;
        var insert = {};
        for (let column of columns) {
            insert[column] = instance[column];
        }
        model.insert(insert, (err, res) => {
            if (!err) {
                //insert local data
                for (let column in insert) {
                    data[column] = insert[column];
                }
                instance.__$$synced = true;
            }
            callback(err);
        });
    }

    /**
     * Save values altered since instantiation
     * @param {function} callback - receives err
     */
    save(callback) {
        var instance = this;
        var synced = instance.__$$synced;
        if (!synced) {
            return instance.sync(callback);
        }
        var model = instance.model;
        var primaries = model.primaries;
        var schema = model.schema;
        var data = instance.__$$object;
        var columns = schema.columns;
        var update = {};
        var where = {};
        for (let column of columns) {
            if (primaries[column]) {
                where[column] = data[column];
            }
            if (data[column] !== instance[column]) {
                update[column] = instance[column];
            }
        }
        model.update(where, update, (err, res) => {
            if (!err) {
                //update local data
                for (let column in update) {
                    data[column] = update[column];
                }
            }
            callback(err);
        });
    }

    /**
     * Delete columns or entire rows from a model instance
     * @param {array} projection - an array of columns to delete [optional]
     * @param {function} callback - receives err
     */
    delete(projection, callback) {
        var instance = this;
        var model = instance.model;
        var data = instance.__$$object;
        var keys = model.primaryKeys;
        var where = {};
        if (projection && typeof projection === 'function') {
            callback = projection;
            projection = null;
        }
        var callbackHandler = (err, result) => {
                if (!err) {
                    instance.__$$synced = false;
                }
                callback(err);
            };
        for (let key of keys) {
            where[key] = data[key];
        }
        if (projection) {
            model.delete(where, projection, callbackHandler);
        } else {
            model.delete(where, callbackHandler);
        }

    }
}


module.exports = ModelInstance;
