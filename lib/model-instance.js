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
    }
}


module.exports = ModelInstance;
