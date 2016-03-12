'use strict';
const assert = require('assert');
const Cassandra = require('../');
const keyspaceName = 'testkeyspace';
const async = require('async');
const format = require('util').format;
const keyspaceConfig = {
    withReplication: {
        class: 'SimpleStrategy',
        replication_factor: 1
    },
    durableWrites: true
};
const config = {
    contactPoints: ['127.0.0.1:9042'], 
    protocolOptions: {port: 9042},
    keyspace: {}
};

config.keyspace[keyspaceName] = keyspaceConfig;
var cassandra;

describe('Cassandra >', function (done) {

    this.timeout(5000);

    before((done) => {
        cassandra = Cassandra.connect(config);
        cassandra.on('error', done);
        cassandra.on('connect', done);
    });
    after((done) => {
        cassandra.driver.execute('DROP KEYSPACE ' + cassandra.keyspace, done);
    });
    describe('Keyspace >', () => {
        it ('should be able to create keyspaces if they don\'t exist', (done) => {
            cassandra.driver.metadata.refreshKeyspace('testkeyspace', (err, result) => {
                if (err) {
                    return done(err);
                }
                assert.equal(result.name, keyspaceName);
                done();
            });
        });
        it ('should set the "keyspace" property on Cassandra', () => {
            assert(cassandra.keyspace, 'testkeyspace');
        });
    });

    describe('Schema >', () => {
        describe('Keys >', () => {
            it ('should create a Schema object with single partition key', () => {
                assert.doesNotThrow(() => {
                    new Cassandra.Schema({
                        username: 'text',
                        age: 'int'
                    }, {
                        primaryKeys: ['age']
                    });
                });
            });
            it ('should create a Schema object with compound keys', () => {
                assert.doesNotThrow(() => {
                    new Cassandra.Schema({
                        username: 'text',
                        age: 'int'
                    }, {
                        primaryKeys: ['age', 'username']
                    });
                });
            });
            it ('should create a Schema object with composite keys', () => {
                assert.doesNotThrow(() => {
                    new Cassandra.Schema({
                        name: 'text',
                        username: 'text',
                        age: 'int'
                    }, {
                        primaryKeys: [['age', 'name'], 'username']
                    });
                });
            });
        });
        describe('Validation >', () => {
            it ('should fail at creating schema if there are no primaryKeys', () => {
                assert.throws(() => {
                    new Cassandra.Schema({username: 'text', age: 'int'});
                });
            });
            it ('should fail at qualifying a schema if the object type is not supported', () => {
                assert.throws(() => {
                    new Cassandra.Schema({username: 'foo', age: 'bar'});
                });
            });
            it ('should fail at qualifying a schema if a view specifies an undeclared column in the "primaryKeys" array', () => {
                assert.throws(() => {
                    new Cassandra.Schema({
                        username: 'text', 
                        age: 'int'
                    },{
                        primaryKeys: ['age'],
                        views: {
                            test: {
                                primaryKeys: ['foo']
                            }
                        }
                    });
                });
            });
            it ('should fail at qualifying a schema if the views specifies an undeclared column in the "select" array', () => {
                assert.throws(() => {
                    new Cassandra.Schema({
                        name: 'text',
                        username: 'text', 
                        age: 'int'
                    },{
                        primaryKeys: ['age'],
                        views: {
                            test: {
                                select: ['foo'],
                                primaryKeys: ['name']
                            }
                        }
                    });
                });
            });
            it ('should fail at qualifying a schema if the views specifies an undeclared column in the "orderBy" array', () => {
                assert.throws(() => {
                    new Cassandra.Schema({
                        name: 'text',
                        username: 'text', 
                        age: 'int'
                    },{
                        primaryKeys: ['age'],
                        views: {
                            test: {
                                primaryKeys: ['name'],
                                orderBy: {
                                    foo: 'asc'
                                }
                            }
                        }
                    });
                });
            });
        });
    });

    describe('Model >', () => {
        var testPartition,
            testPartitionModel, 
            testCompound,
            testCompoundModel,
            testComposite,
            testCompositeModel;
        before(() => {
            testPartition = new Cassandra.Schema({
                username: 'text',
                age: 'int'
            }, {
                primaryKeys: ['username'] //single partition key
            });
            testCompound = new Cassandra.Schema({
                username: 'text',
                name: 'text',
                age: 'int'
            }, {
                primaryKeys: ['username', 'age', 'name'] //compound keys
            });
            testComposite = new Cassandra.Schema({
                username: 'text',
                name: 'text',
                age: 'int'
            }, {
                primaryKeys: [['username', 'name'], 'age'] //composite keys
            });
        });
        after(() => {
            async.parallel([
                (next) => cassandra.driver.execute(format('DROP TABLE %s.%s', cassandra.keyspace, testPartitionModel.qualifiedName), next),
                (next) => cassandra.driver.execute(format('DROP TABLE %s.%s', cassandra.keyspace, testCompositeModel.qualifiedName), next),
                (next) => cassandra.driver.execute(format('DROP TABLE %s.%s', cassandra.keyspace, testCompoundModel.qualifiedName), next)
            ], done);
        });
        it ('should be able to attach a model to the db instance', (done) => {
            async.parallel([
                (next) => testPartitionModel = cassandra.model('testpartition', testPartition, next),
                (next) => testCompoundModel = cassandra.model('testcompound', testCompound, next),
                (next) => testCompositeModel = cassandra.model('testcomposite', testComposite, next)
            ], done);
        });
        it ('should create a table "testpartition" with a single partition key', (done) => {
            cassandra.driver.metadata.getTable(cassandra.keyspace, testPartitionModel.name, (err, table) => {
                if (err) {
                    return done(err);
                }
                assert.equal(table.name, testPartitionModel.name);
                assert.equal(table.partitionKeys.length, 1);
                assert.equal(table.partitionKeys[0].name, testPartition.options.primaryKeys[0]);
                done();
            });
        });
        it ('should create a table "testcompound" with a compound key', (done) => {
            cassandra.driver.metadata.getTable(cassandra.keyspace, testCompoundModel.name, (err, table) => {
                if (err) {
                    return done(err);
                }
                assert.equal(table.name, testCompoundModel.name);
                assert.equal(table.partitionKeys.length, 1);
                assert.equal(table.clusteringKeys.length, 2);
                assert.equal(table.partitionKeys[0].name, testCompound.options.primaryKeys[0]);
                assert.equal(table.clusteringKeys[0].name, testCompound.options.primaryKeys[1]);
                assert.equal(table.clusteringKeys[1].name, testCompound.options.primaryKeys[2]);
                done();
            });
        });
        it ('should create a table "testcomposite" with a composite key', (done) => {
            cassandra.driver.metadata.getTable(cassandra.keyspace, testCompositeModel.name, (err, table) => {
                if (err) {
                    return done(err);
                }
                assert.equal(table.name, testCompositeModel.name);
                assert.equal(table.partitionKeys.length, 2);
                assert.equal(table.partitionKeys[0].name, testComposite.options.primaryKeys[0][0]);
                assert.equal(table.partitionKeys[1].name, testComposite.options.primaryKeys[0][1]);
                assert.equal(table.clusteringKeys[0].name, testComposite.options.primaryKeys[1]);
                done();
            });

        });

        describe('Static Methods >', () => {
            var schema;
            var testSchema, TestModel;
            before((done) => {
                schema = new Cassandra.Schema({
                    name: 'text',
                    age: 'int',
                    username: 'text'
                },{
                    primaryKeys: ['name']
                });
                testSchema = new Cassandra.Schema({
                    username: 'text',
                    age: 'int',
                    name: 'text'
                }, {
                    primaryKeys: ['username', 'age'],
                    views: {
                        byName: {
                            //select: ['name', 'username'] is implied
                            primaryKeys: ['name'],//, 'username'], is implied
                            orderBy: {
                                name: 'asc'
                            }
                        }
                    }
                });
                TestModel = cassandra.model('teststatic', testSchema, done);
            });
            after((done) => {
                async.series([
                    (next) => cassandra.driver.execute(format(
                            'DROP TABLE %s.%s', 
                            cassandra.keyspace, 
                            'teststatic2'), next),
                    (next) => cassandra.driver.execute(format(
                            'DROP MATERIALIZED VIEW %s.%s', 
                            cassandra.keyspace, 
                            TestModel.name + '__byname'), next),
                    (next) => cassandra.driver.execute(format(
                            'DROP TABLE %s.%s', 
                            cassandra.keyspace, 
                            TestModel.name), next)
                ], done);
            });
            it ('should be able to attach static methods to a schema that persist onto the model', (done) => {
                schema.statics.findFoo = function (callback) {
                    return this.findOne({name: 'foo'}, callback);
                };
                var TestModel2 = cassandra.model('teststatic2', schema, () => {
                    TestModel2.insert({name: 'foo', age: 20, username: 'bar'}, (err, result) => {
                        if (err) {
                            return done(err);
                        }
                        TestModel2.findFoo((err, result) => {
                            if (err) {
                                return done(err);
                            }
                            assert(!result.length, 'row result should not be an array');
                            assert.equal(result.name, 'foo');
                            done();
                        });
                    });
                });
            });

            describe('Insert >', () => {
                it ('should be able to perform a basic insert', (done) => {
                    async.parallel([
                        (next) => TestModel.insert({username: 'foo', age: 30, name: 'bar'}, next),
                        (next) => TestModel.insert({username: 'foo', age: 31, name: 'bazz'}, next),
                        (next) => TestModel.insert({username: 'baz', age: 32, name: 'bars'}, next)
                    ], done);
                });
                it.skip ('should be able to insert list types', () => {
                });
                it.skip ('should be able to insert map types', () => {
                });
                it.skip ('should be able to insert set types', () => {
                });
            });
            
            describe('FindOne >', () => {
                it ('should be able to find a single row and return an object', (done) => {
                    TestModel.findOne({username: 'foo'}, (err, row) => {
                        if (err) {
                            done(err);
                        } else {
                            assert.equal(row.username, 'foo');
                            done();
                        }
                    });
                }); 
                it ('should return null result when trying to find a single a single non-existent row', (done) => {
                    TestModel.findOne({username: 'fsoo'}, (err, row) => {
                        if (err) {
                            done(err);
                        } else {
                            assert.equal(row, null);
                            done();
                        }
                    });
                }); 
            });

            describe('Find >', () => {//{{{
                it ('should be able perform a basic find with queryObject', (done) => {
                    TestModel.find({username: 'foo', age: 30}, (err, rows) => {
                        if (err) {
                            return done(err);
                        }
                        assert.equal(rows.length, 1);
                        assert.equal(rows[0].username, 'foo');
                        assert.equal(rows[0].age, 30);
                        done();
                    });
                });
                it ('should be able perform a complex find with queryObject and operators', (done) => {
                    TestModel.find({
                        username: 'foo',
                        age: {
                            $gt: 30, 
                            $lt: 50
                        }
                    }, (err, rows) => {
                        if (err) {
                            return done(err);
                        }
                        assert.equal(rows.length, 2);
                        assert.equal(rows[0].username, 'foo');
                        assert.equal(rows[1].username, 'foo');
                        assert.equal(rows[0].age, 30);
                        assert.equal(rows[1].age, 31);
                        done();
                    });
                });
                it ('should be able perform a complex find with queryObject and filters', (done) => {
                    TestModel.find({
                        username: 'foo',
                        age: {
                            $in: [29, 30, 31]
                        }
                    }, (err, rows) => {
                        if (err) {
                            return done(err);
                        }
                        assert.equal(rows.length, 2);
                        assert.equal(rows[0].username, 'foo');
                        assert.equal(rows[1].username, 'foo');
                        assert.equal(rows[0].age, 30);
                        assert.equal(rows[1].age, 31);
                        done();
                    });
                });
                it ('should be able to return results based off a projection object', (done) => {
                    TestModel.find({username: 'foo', age: 30}, ['name'], (err, rows) => {
                        if (err) {
                            return done(err);
                        }
                        assert.equal(rows.length, 1);
                        assert(!rows[0].username);
                        assert(!rows[0].age);
                        assert.equal(rows[0].name, 'bar');
                        done();
                    });
                });
                it ('should be able perform a complex find and filter limit', (done) => {
                    TestModel.find({
                        username: 'foo'
                    }, {
                        limit: 1
                    }, (err, rows) => {
                        if (err) {
                            return done(err);
                        }
                        assert.equal(rows.length, 1);
                        assert.equal(rows[0].username, 'foo');
                        assert.equal(rows[0].age, 30);
                        done();
                    });
                });
                it.skip ('should be able to find list types', () => {
                });
                it.skip ('should be able to find map types', () => {
                });
                it.skip ('should be able to find set types', () => {
                });
            });//}}}

            describe('Update >', () => {
                it ('should return an error attempting to update a primary key', (done) => {
                    TestModel.update({username: 'foo', age: 30}, {age: 33}, (err, rows) => {
                        assert(err, 'Error not found, should have returned an error');
                        if (err) {
                            done();
                        }
                    });
                });
                it ('should be able to perform a basic update on a single value', (done) => {
                    TestModel.update({username: 'foo', age: 30}, {name: 'test'}, (err, rows) => {
                        done(err);
                    });
                });
                it.skip ('should be able to update list types', () => {
                });
                it.skip ('should be able to update map types', () => {
                });
                it.skip ('should be able to update set types', () => {
                });
            });

            describe('Delete >', () => {//{{{
                it ('should be able to delete row(s) by query', (done) => {
                    var query = {
                            username: 'foo', 
                            age: 30
                        };
                    TestModel.delete(query, (err, result) => {
                        if (err) {
                            done(err);
                        } else {
                            TestModel.find(query, (err, result) => {
                                if (err) {
                                    done(err);
                                } else {
                                    assert.equal(result, null);
                                    done();
                                }
                            });
                        }
                    });
                });
                it.skip ('should be able to delete list types', () => {
                });
                it.skip ('should be able to delete map types', () => {
                });
                it.skip ('should be able to delete set types', () => {
                });
            });//}}}
        });
    });

    describe('Model Instances >', () => {
        var schema, UserModel;
        before((done) => {
            schema = new Cassandra.Schema({
                hex: {
                    type: 'uuid',
                    default: Cassandra.uuid
                    //required: true - required is implied with default
                },
                names: {
                    type: 'text',
                    required: true
                },
                age: {
                    type: 'int',
                    default: 50
                },
                username: 'text'
            },{
                primaryKeys: ['names']
            });
            UserModel = cassandra.model('testinstance', schema, done);
        });
        after((done) => {
            cassandra.driver.execute(format(
                'DROP TABLE %s.%s', 
                cassandra.keyspace, 
                UserModel.name
            ), done);
        });
        it ('should only enumerate properties set by the validated object during instantiation', () => {
            var user = new UserModel({
                    hex: Cassandra.uuid(),
                    names: 'baz',
                    username: 'fii'
                });
            assert.deepEqual(Object.keys(user), ['age', 'hex', 'names', 'username'], 'did not properly enumerate object');
        });
        describe('Validation >', () => {
            it ('should fail at creating a new ModelInstance without a required field ', () => {
                assert.throws(() => {
                    new UserModel({
                        age: 24, 
                        username: 'fii'
                    });
                });
            });
            it ('should automatically set default values of type "function" by calling the function', () => {
                var user = new UserModel({
                        names: 'baz',
                        age: 24, 
                        username: 'fii'
                    });
                assert(user.hex instanceof Cassandra.types.Uuid, 'not a valid guid');
            });
            it ('should automatically set default values of type "function" by calling the function', () => {
                var user = new UserModel({
                        names: 'baz',
                        age: 24, 
                        username: 'fii'
                    });
                assert(user.hex instanceof Cassandra.types.Uuid, 'not a valid guid');
            });
            it ('should automatically set default values of all other types by copy', () => {
                var user = new UserModel({
                        hex: Cassandra.uuid(),
                        names: 'baz',
                        username: 'fii'
                    });
                assert.equal(user.age, 50, 'did not properly set default age value');
            });
            it.skip ('should be able to set null values', () => {
            });
        });
    });

    describe('Materialized Views >', () => {//{{{
        var testSchema, TestModel;
        before(() => {
            testSchema = new Cassandra.Schema({
                username: 'text',
                age: 'int',
                name: 'text'
            }, {
                primaryKeys: ['username'],
                views: {
                    byName: {
                        //select: ['name', 'username'] is implied
                        primaryKeys: ['name'],//, 'username'], is implied
                        orderBy: {
                            name: 'asc'
                        }
                    },
                    byAge: {
                        select: ['name'],
                        primaryKeys: ['age'],//, 'username'], is implied
                        orderBy: {
                            age: 'asc'
                        }
                    }
                }
            });
        });
        after((done) => {
            async.series([
                (next) => cassandra.driver.execute(format(
                        'DROP MATERIALIZED VIEW %s.%s', 
                        cassandra.keyspace, 
                        TestModel.name + '__byname'), next),
                (next) => cassandra.driver.execute(format(
                        'DROP MATERIALIZED VIEW %s.%s', 
                        cassandra.keyspace, 
                        TestModel.name + '__byage'), next),
                (next) => cassandra.driver.execute(format(
                        'DROP TABLE %s.%s', 
                        cassandra.keyspace, 
                        TestModel.name), next)
            ], done);
        });
        it ('should be able to create views and attach them to the model', (done) => {
            TestModel = cassandra.model('testschema', testSchema, (err, rows) => {
                if (err) {
                    return done(err);
                }
                setTimeout(() => {
                    cassandra.driver.execute('SELECT view_name FROM system_schema.views WHERE keyspace_name = \''
                            + cassandra.keyspace + "'", (err, result) => {
                        if (err) {
                            return done(err);
                        }
                        var rows = result.rows;
                        assert.equal(rows.length, 2);
                        assert.equal(rows[0].view_name, TestModel.views.byAge.qualifiedName);
                        assert.equal(rows[1].view_name, TestModel.views.byName.qualifiedName);
                        done();
                    });
                }, 200);
            });
        });
        
        describe('Find >', () => {
            before((done) => {
                TestModel.insert({username: 'foo', age: 30, name: 'bar'}, done);
            });

            it ('should be able to find rows specific to the materialized view and return an array', (done) => {
                TestModel.views.byName.find({name: 'bar'}, (err, row) => {
                    if (err) {
                        done(err);
                    } else {
                        assert.equal(row.length, 1, 'row result is not an array');
                        assert.equal(row[0].name, 'bar', 'did not find the right row');
                        assert.equal(Object.keys(row[0])[0], 'name', 'did not use the right order/likely wrong column family used');
                        done();
                    }
                });
            });
        });

        describe('FindOne >', () => {
            before((done) => {
                TestModel.insert({username: 'foo', age: 30, name: 'bar'}, done);
            });

            it ('should be able to find a single row and return an object', (done) => {
                TestModel.views.byName.findOne({name: 'bar'}, (err, row) => {
                    if (err) {
                        done(err);
                    } else {
                        assert.equal( ! Array.isArray(row), 1, 'row result should not be an array');
                        assert.equal(row.name, 'bar', 'did not find the right row');
                        assert.equal(Object.keys(row)[0], 'name', 'did not use the right order/likely wrong column family used');
                        done();
                    }
                });
            });
        });
    });//}}}

    it.skip ('should convert uuid types from strings to objects or fail', () => {
    });
    it.skip ('should be able to create a table with table options object', () => {
    });
});
