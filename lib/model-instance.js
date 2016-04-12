/**
 * Vertebrae Inc
 * @package Cassandra-ORM
 * @exports Model
 */
"use strict";

const format = require('util').format;

const defaultTypeMap = {
    set: Array,
    list: Array,
    map: Object
};
/**
 * Creates a new ModelInstance object based on your model's schema, this
 * class extends the model.Factory class object, which is what's returned
 * everytime you instantiate a new Model(ColumnFamily)
 * @memberof Cassandra.Model
 * @param {object} dataObject - an object containing column:values that will initialize the instance
 * @param {object} model - the model this instance is based off of
 */
class ModelInstance {

    constructor(object, model, bypass) {
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

        instance._prepareObject(bypass);
    }

    /**
     * This is called at instantiation of each new ModelInstance
     * and will store the qualified data in a secret place.
     * This data store and the instance's enumerable properties(columns)
     * is updated on MOST successful actions except for when deleting
     * the entire object from the table, then the data is left intact.
     * {@link Cassandra.Model.ModelInstance#restore|restore()} will restore the instances
     * column values to the values in the data store;
     * @params {boolean} bypass - do not check defaults/required, just set object's values
     */
    _prepareObject(bypass) {
        var instance = this,
            save = {},
            object = instance.object,
            model = instance.model,
            schema = model.schema,
            columns = schema.columns,
            columnMap = schema.columnMap,
            required = schema.required,
            defaults = schema.defaults;
        for (let column of columns) {
            //if bypass, use mapped key for result parsing
            let mappedKey = columnMap[column];
            let type = schema.model[column];
            let objectValue = bypass ? object[mappedKey] : object[column];
            if (bypass) {
                if (!objectValue && defaultTypeMap[type]) {
                    objectValue = new defaultTypeMap[type]();
                }
                instance[column] = save[column] = objectValue;
                continue;
            }
            if (undefined !== objectValue) {
                save[column] = objectValue;
            } else {
                let defaultVal = defaults[column];
                //if a default is set and no value is provided
                if (undefined !== defaultVal) {
                    //if it has a apply property
                    if (typeof defaultVal === 'function') {
                        save[column] = defaultVal();
                    } else {
                        save[column] = defaultVal;
                    }
                } else if (!bypass && required[column]) {
                    throw new Error(format(
                        'Column value is required > column: %s, columnFamily: %s',
                        column,
                        model.name
                    ));
                } else if (!objectValue && defaultTypeMap[type]) {
                    save[column] = new defaultTypeMap[type]();
                }
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
     * or are saving an object for the first time with {@link Cassandra.Model.ModelInstance#save|save()}
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
     * Delete columns or entire rows from a model instance. When deleting
     * a deleteObject of columns, the projected columns will be set to null
     * on the instance and the data store. This is essentially the same as
     * setting values to null and using save.
     * Currently, this method doesn't support using timestamps but it will soon
     * @todo support @usingTimestamp/delete options
     * @param {array} deleteObject - an array of columns to delete [optional]
     * @param {function} callback - receives err
     */
    delete(deleteObject, callback) {
        var instance = this;
        var model = instance.model;
        var data = instance.__$$object;
        var keys = model.primaryKeys;
        var where = {};
        if (deleteObject && typeof deleteObject === 'function') {
            callback = deleteObject;
            deleteObject = null;
        }
        var callbackHandler = (err, result) => {
                if (!err) {
                    instance.__$$synced = false;
                    //only update deleteObject fields, leave data intact
                    if (deleteObject) {
                        for (let column in deleteObject) {
                            data[column] = null;
                        }
                    }
                }
                callback(err);
            };
        for (let key of keys) {
            where[key] = data[key];
        }
        if (deleteObject) {
            model.delete(where, deleteObject, callbackHandler);
        } else {
            model.delete(where, callbackHandler);
        }
    }
}


module.exports = ModelInstance;
