/**
 * Vertebrae Inc
 * @package Cassandra-ORM
 * @exports Model
 * @exports Model.TypeMaps
 * @exports Model.BaseModel
 * @exports Model.AbstractModel
 */
"use strict";

const Cassandra = require('./cassandra');
const util = require('util');
const format = util.format;

const OperatorMap = {
    $gt: '>',
    $gte: '>=',
    $lt: '<',
    $lte: '<=',
    $eq: '='
};


const TypeMap = {};
const dataTypes = Cassandra.types.dataTypes;
Object.keys(dataTypes).forEach((keys, index) => {
    TypeMap[keys] = index + 5;
});
//Mapping collection types for better expressions, only collections will be less than 4
TypeMap.list = 1;
TypeMap.set = 2;
TypeMap.map = 3;

const FilterMap = {
    $in: 'IN',
    $contains: 'CONTAINS',
    $containsKey: 'CONTAINS KEY'
};

const getArgCount = Cassandra.utils.getArgCount;
/**
 * Create a new Cassandra Model attached to a {@link Cassandra.Schema} on the named table and creating the table if it doesn't exist. In general, you should use the instanced method {@link Cassandra#model} to attach models
 * @memberof Cassandra
 * @todo Model.toObject()
 * @param {object} db - the database connection instance
 * @param {string} name - the model name, will become the name of the column family
 * @param {object} schema - the {@link Cassandra.Schema} to be attached
 * @param {function} callback - a callback to be called once the table has been created
 * @example <caption>Attach a schema to a model for querying</caption>
 * var cassandra = Cassandra.connect(...);
 * var testSchema = new Cassandra.Schema(...);
 * var testModel = new Cassandra.Model(cassandra, 'test', testSchema);
 */
class Model {

    constructor(db, name, schema) {
        var model = this;
        var baseModel;
        model.name = name;
        model.db = db;
        model.isColumnFamily = model instanceof Model.ColumnFamily;
        model.isMaterializedView = !model.isColumnFamily;

        if (!(db instanceof Cassandra)) {
            throw new TypeError('Model expects parameter 1 to be an '
                + 'instanceof Cassandra');
        }
        if (!name || !name.length) {
            throw new TypeError('Model expects parameter 2 to be of type '
                + '"string": @"' + name + '"');
        }
        //is instance a materialized view
        if (model.isColumnFamily) {
            model.qualifiedName = name.toLowerCase();
        } else {
            baseModel = schema;
            schema = baseModel.schema;
            model.qualifiedName = baseModel.qualifiedName + '__' + name.toLowerCase();
        }
        if (!(schema instanceof Cassandra.Schema)) {
            throw new TypeError('Model expects parameter 3 to be an '
                + 'instance of Cassandra.Schema: "' + schema + '"');
        }
        model.__$$built = false;
        model.schema = schema;
        model.primaryKeys = schema.options.primaryKeys;
        var primaries = model.primaries = {};
        model.primaryKeys.forEach((key) => {
            primaries[key] = 1;
        });
    }

    /**
     * Given a primary key array object, this will convert it to a
     * PRIMARY KEY (...) string value to use in table creation
     * @param {array} primaryKeyArray - An array specifying the primary key columns, can be compound and composite as well
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
     * Checks the queryObject's properties against the qualified columns
     * @param {object} queryObject - an object representing column:value
     * @throws Error - not a valid query column
     * @returns {array} columns - a list of qualified columns
     */
    _qualifyQueryColumns(queryObject) {
        var model = this;
        var schema = model.schema;
        var columns = [];
        for (let field in queryObject) {
            if (!schema.model[field]) {
                throw new Error(format('Not a valid queryObject, could not find '
                    + 'column: %s, model: %s', field,  model.name));
            }
            columns.push(field);
        }
        return columns;
    }

    _buildQueryComponents(queryObject) {
        var model = this;
        var values = [];
        var where = [];
        var columns = model._qualifyQueryColumns(queryObject);
        //var columns = Object.keys(queryObject);
        for (let column of columns) {
            let value = queryObject[column];
            //make sure this isn't a String or Number, but should be an object
            if (value.length || !isNaN(value) || value instanceof Cassandra.types.Uuid) {
                values.push(value);
                where.push(column + '=?');
                continue;
            }
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
        }
        return {
            where: where.join(' AND '),
            values: values,
            columns: columns
        };
    }
}



/**
 * BaseModel class extends the {@link Cassandra.Model|Model} class
 * and implements basic CRUD ops from {@link AbstractModel}
 * @extends Cassandra.Model
 */
class BaseModel extends Model {}



/**
 * AbstractModel class exposes basic CRUD ops for models.
 * This class should not be instantiated by itself.
 * @mixin
 */
class AbstractModel {

    /**
     * Insert items into the model's column family
     * @param {object} queryObject - an object representing column:value
     * @param {function} callback - receives err, result
     */
    insert(queryObject, callback) {
        var model = this;
        var cassandra = model.db;
        var columns = model._qualifyQueryColumns(queryObject);
        var fieldSize = columns.length;
        var values = columns.map((key) => queryObject[key]);
        var marks = new Array(fieldSize).join('?,') + '?';
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
     * Find items in the model's column family
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
        var argc = getArgCount(arguments);
        if (argc === 3) {
            callback = options;
            //if projection is an object then it's the options object
            if (!Array.isArray(projection)) {
                options = projection;
                projection = null;
            }
        } else if (argc === 2) {
            callback = projection;
            projection = null;
        } else if (argc === 1) {
            callback = queryObject;
            queryObject = null;
        }
        if (typeof callback !== 'function') {
            throw new Error('Callback must be a function!');
        }
        var findOne = false;
        var model = this;
        var raw = false;
        var Factory = model.Factory;
        var queryComponents = model._buildQueryComponents(queryObject);
        var cassandra = model.db;
        var query;
        if (queryComponents.where.length > 0) {
            query = format(
                'SELECT %s FROM %s.%s WHERE %s',
                projection ? projection.join(', ') : '*',
                cassandra.keyspace,
                model.qualifiedName,
                queryComponents.where
            );
        } else {
            query = format(
                'SELECT %s FROM %s.%s',
                projection ? projection.join(', ') : '*',
                cassandra.keyspace,
                model.qualifiedName
            );
        }
        if (options) {
            if (options.__$$findOne) {
                findOne = true;
            }
            if (options.limit) {
                query += ' LIMIT ' + options.limit;
            }
            if (options.allowFiltering) {
                query += ' ALLOW FILTERING';
            }
            if (options.raw) {
                raw = !!options.raw;
            }
            if (options.stream) {
                return cassandra.driver.stream(query, queryComponents.values);
            }
            if (options.pipe) {
                cassandra.driver.eachRow(
                    query,
                    queryComponents.values,
                    (n, row) => callback(null, row, n),
                    (err) => callback(err)
                );
                return;
            }
        }
        cassandra.driver.execute(query, queryComponents.values, {prepare: true}, (err, result) => {
            if (err) {
                return callback(err);
            }
            if (!result.rows.length) {
                result = null;
            } else if (!raw) {
                let rows = result.rows;
                let size = result.rowLength;
                result = new Array(size);
                for (let i = 0; i < size; i++) {
                    //create object, bypass defaults/required
                    result[i] = new Factory(rows[i], true);
                }
            } else {
                result = result.rows;
                var dataMap = model.schema.dataMap;
                let rowKeys = Object.keys(result[0]);
                for (let row of result) {
                    for (let key of rowKeys) {
                        let mapping = dataMap[key];
                        if (key !== mapping) {
                            row[mapping] = row[key];
                            delete row[key];
                        }
                    }
                }
            }
            callback(null, result);
        });
    }

    /**
     * Update items in the model's column family
     * @param {object} queryObject - an object representing column:value
     * @param {object} updateObject - an update object that uses $gt, $gte, $lt, $lte, $eq, $in, $contains, $containsKey
     * @param {function} callback - receives err, result
     * @example
     * var query = {
     *   name: 'foo',
     *   age: {
     *     $in: [29,30,31]
     *   }
     * };
     * var update = {
     *   name: 'bar'
     * };
     * cassandra.find(query, projection, options, (err, result) => console.log(err, result));
     * // UPDATE <table> SET name='bar' WHERE name = 'foo' AND age IN (29, 30, 31)
     */
    update(queryObject, updateObject, callback) {
        var model = this;
        var cassandra = model.db;
        var dataModel = model.schema.model;
        var queryComponents = model._buildQueryComponents(queryObject);
        var set = [];
        var setValues = [];
        for (let column in updateObject) {
            let typeMapping = TypeMap[dataModel[column]];
            let value = updateObject[column];
            //is this a special operator character for set,list
            if (typeMapping < TypeMap.map && !Array.isArray(value)) {
                for (let action in value) {
                    if (action === '$append' || action === '$add') {
                        set.push(column + ' = ' + column + ' + ?');
                        setValues.push(value[action]);
                    } else if (action === '$prepend') {
                        set.push(column + ' = ? + ' + column);
                        setValues.push(value[action]);
                    } else if (action === '$filter') {
                        set.push(column + ' = ' + column + ' - ?');
                        setValues.push(value[action]);
                    } else if (action === '$set') {
                        let obj = value[action];
                        for (let index in obj) {
                            set.push(column + '[' + index + ']' + ' = ?');
                            setValues.push(obj[index]);
                        }
                    }
                }
            } else if (typeMapping === TypeMap.map) {
                //reset the entire map object to the $set object
                if (value.$set) {
                    set.push(column + '=?');
                    setValues.push(updateObject[column].$set);
                } else {
                    //[setKey: setValue] = value[action];
                    for (let setKey in value) {
                        set.push(column + '[?] = ?');
                        setValues.push(setKey, value[setKey]);
                    }
                }
            } else {
                //plain value, assume set
                set.push(column + '=?');
                setValues.push(updateObject[column]);
            }
        }
        var query = format(
                'UPDATE %s.%s SET %s WHERE %s',
                cassandra.keyspace,
                model.name,
                set,
                queryComponents.where
            );
        cassandra.driver.execute(query, setValues.concat(queryComponents.values), {prepare: true}, callback);
    }

    /**
     * Delete columns or rows in the model's column family
     * @param {object} queryObject - an object representing column:value
     * @param {array} deleteObject - deleteObject for selecting a subset of columns in select statements
     * @param {function} callback - receives err, result
     * @example
     * var deleteObject = ['name']; //default "*"
     * var deleteObject = {
     *   usingTimestamp: Date.now()
     * };
     * var query = {
     *   name: 'foo',
     *   age: {
     *     $gt: 30
     *   }
     * };
     * cassandra.delete(query, deleteObject, (err, result) => console.log(err, result));
     * // DELETE name FROM <table> WHERE name = 'foo' AND age > 30
     */
    delete(queryObject, deleteObject, callback) {
        var argc = getArgCount(arguments);
        if (argc === 2) {
            callback = deleteObject;
            deleteObject = null;
        }
        if (typeof callback !== 'function') {
            throw new Error('Callback must be a function!');
        }
        var model = this;
        var dataModel = model.schema.model;
        var queryComponents = model._buildQueryComponents(queryObject);
        var cassandra = model.db;
        var unset = [];
        var unsetValues = [];
        for (let column in deleteObject) {
            let type = dataModel[column];
            let typeMapping = TypeMap[type];
            //is this a special operator eg: $usingTimestamp, in which case
            //it won't have a type mapping
            if (!type) {
                continue;
            }
            let value = deleteObject[column];
            //check if list type is an object with $pull operator
            if (typeMapping === TypeMap.list && Array.isArray(value)) {
                unset.push(value.map((index) => column + '[' + index + ']'));
            } else if (typeMapping === TypeMap.map && Array.isArray(value)) {
                unset.push(value.map((index) => {
                    unsetValues.push(index);
                    return column + '[?]';
                }));
            } else {
                unset.push(column);
            }
        }
        var query = 'DELETE ' + (unset.length ? unset + ' ' : '')
            + 'FROM ' + cassandra.keyspace + '.' + model.name;
        if (deleteObject) {
            if (deleteObject.$usingTimestamp) {
                query += ' USING TIMESTAMP ' + deleteObject.$usingTimestamp;
            }
        }
        query += ' WHERE ' + queryComponents.where;
        cassandra.driver.execute(query, unsetValues.concat(queryComponents.values), {prepare: true}, callback);
    }

    /**
     * Same as {@link Cassandra.Model.find} except forces a limit of 1
     * and always returns null or a row object
     */
    findOne(queryObject, projection, options, callback) {
        var argc = getArgCount(arguments);
        if (argc === 3) {
            callback = options;
            //if projection is an object then it's the options object
            if (!Array.isArray(projection)) {
                options = projection;
                projection = null;
            }
        } else if (argc === 2) {
            callback = projection;
            projection = null;
        }
        options = options || {};
        options.limit = 1;
        options.__$$findOne = true;
        AbstractModel.prototype.find.apply(this, [queryObject, projection, options, (err, result) => {
            if (Array.isArray(result)) {
                result = result.shift();
            }
            callback(err, result);
        }].filter(Boolean));
    }

}


//mixin abstract model
var abstractMethods = Object.getOwnPropertyNames(AbstractModel.prototype);
for (let method of abstractMethods) {
    if (method === 'constructor') {
        continue;
    }
    BaseModel.prototype[method] = AbstractModel.prototype[method];
}


Model.TypeMap = TypeMap;
Model.BaseModel = BaseModel;
Model.AbstractModel = AbstractModel;

module.exports = Model;
