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
    contactPoints: ['127.0.0.1'],
    protocolOptions: {port: 9042},
    keyspace: {}
};

config.keyspace[keyspaceName] = keyspaceConfig;
const cassandra = new Cassandra(config);

describe('Cassandra Class >', () => {
    var schema, TestModel;
    it ('should be able to create schemas without a connection', () => {
        schema = new Cassandra.Schema({
            foo: 'text',
            fii: 'text',
            bar: 'text',
            baz: 'text',
            name: 'text',
            username: 'text'
        }, {
            primaryKeys: ['foo'],
            indexes: ['baz'],
            views: {
                byName: {
                    primaryKeys: ['name']
                }
            }
        });
    });
    it ('should be able to queue and instantiate models without a connection', () => {
        TestModel = cassandra.model('testmodelfoo', schema);
    });
    it ('should be able to queue and instantiate indexes without a connection', () => {
        TestModel.model.createIndex('fii');
        assert(TestModel.model.indexes.fii, format('%s_%s_idx', TestModel.name, 'fii'));
    });
    it ('should be able to queue and instantiate indexes with custom names without a connection', () => {
        TestModel.model.createIndex({bar: 'test_model_index'});
        assert(TestModel.model.indexes.bar, 'test_model_index');
    });
    it ('should be able to queue and instantiate materialized views without a connection', () => {
        TestModel.model.createView('byUsername', {
            primaryKeys: ['username']
        });
        assert(!!TestModel.views.byUsername);
    });
});

describe('Cassandra >', function (done) {

    var origKey;

    this.timeout(5000);

    var connecting = false;
    it('should be able to connect and process a queue', (done) => {
        connecting = true;
        cassandra.on('error', done);
        cassandra.once('connect', () => {
            cassandra.removeListener('error', done);
            done();
        });
        cassandra.connect();
    });
    after((done) => {
        async.parallel([
            (next) => cassandra.driver.execute('DROP KEYSPACE ' + cassandra.keyspace, next),
            (next) => cassandra.driver.execute('DROP KEYSPACE testfail', next)
        ], (err) => {
            //don't care if this fails
            done();
        });
    });
    describe('Connecting >', () => {
        it ('should properly error on failed connection attempts', (done) => {
            var cassandra2 = Cassandra.connect({
                contactPoints: ['127.0.4444.4444'],
                keyspace: {testfail: keyspaceConfig}
            });
            cassandra2.once('connect', () => {
                done(new Error('Cassandra should not have connected'));
            });
            cassandra2.once('error', () => {
                done();
            });
        });
        it ('should be able to open up mutliple connections', (done) => {
            cassandra.connect(done);
        });
        it ('should have type maps for Uuid', () => {
            assert(Cassandra.uuid() instanceof Cassandra.types.Uuid, 'Uuid is not mapped properly');
        });
        it ('should have type maps for TimeUuid', () => {
            assert(Cassandra.timeuuid() instanceof Cassandra.types.TimeUuid, 'TimeUuid is not mapped properly');
        });
    });

    describe('Keyspace >', () => {
        it ('should set the "keyspace" property on Cassandra', () => {
            assert(cassandra.keyspace, 'testkeyspace');
        });
        it ('should fail at connecting without a keyspace', () => {
            assert.throws(() => {
                Cassandra.connect();
            });
        });
        it ('should fail at connecting without a keyspace', () => {
            assert.throws(() => {
                Cassandra.connect({keyspace:{foo:{}}});
            }, (err) => {
                return err instanceof Error
                    && err.message === 'Cassandra connect expects "keyspace" options to provide a withReplication property';
            });
        });
        it ('should be able to attach a new keyspace to an existing connection', (done) => {
            origKey = cassandra.options.keyspace;
            cassandra.options.keyspace = {
                newTestKeyspace: {
                    withReplication: {
                        class: 'SimpleStrategy',
                        replication_factor: 1
                    }
                }
            };
            cassandra.setKeyspace((err, res) => {
                done(err);
            });
        });
        it ('should be able to switch back to an existing keyspace', (done) => {
            cassandra.options.keyspace = origKey;
            assert(cassandra.keyspace, 'testkeyspace');
            cassandra.setKeyspace(done);
        });
        it ('should throw an error trying to clone a bad options object', () => {
            assert.throws(() => {
                var circular = {
                    foo: function () {},
                    keyspace: {
                        foo: {
                            withReplication: {}
                        }
                    }
                };
                circular.circular = circular;
                new Cassandra(circular);
            }, (err) => {
                return err instanceof Error
                    && err.message === 'Invalid options object, could not clone';
            });
        });
        it ('should be able to connect with only a keyspace', (done) => {
            var cassandra2 = Cassandra.connect({keyspace: {testfail: keyspaceConfig}});
            cassandra2.once('connect', () => {
                cassandra2.driver.shutdown();
                done();
            });
            cassandra2.once('error', done);
        });
    });

    describe('Schema >', () => {
        it ('should throw an error creating a schema without primaryKeys', () => {
            assert.throws(() => {
                new Cassandra.Schema({
                    foo: 'text'
                });
            }, (err) => {
                return err instanceof Error
                    && err.message === 'Schema expects have option "primaryKeys" of type array';
            });
        });
        it ('should throw an error preparing columns if no value if passed', () => {
            assert.throws(() => {
                Cassandra.Schema.prototype._prepareColumns();
            }, (err) => {
                return err instanceof Error
                    && err.message === 'Must provide an object structure of your schema';
            });
        });
        it ('should throw an error preparing columns with an invalid data type', () => {
            assert.throws(() => {
                new Cassandra.Schema({
                    badType: 'badType'
                }, {
                    primaryKeys: ['badType']
                });
            }, (err) => {
                return err instanceof TypeError
                    && err.message === 'Cassandra data type not supported: "badType"';
            });
        });
        it ('should properly set required fields', () => {
            var schema = new Cassandra.Schema({
                fooBar: {
                    type: 'text',
                    required: true
                }
            }, {
                primaryKeys: ['fooBar']
            });
            assert(!!schema.required.fooBar);
        });
        it ('should properly set default fields', () => {
            var schema = new Cassandra.Schema({
                fooBar: {
                    type: 'text',
                    default: () => 'fooname'
                }
            }, {
                primaryKeys: ['fooBar']
            });
            assert(!!schema.defaults.fooBar);
            assert.equal(schema.defaults.fooBar(), 'fooname');
        });
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
            it ('should be able to create a schema using list types', () => {
                assert.doesNotThrow(() => {
                    new Cassandra.Schema({
                        name: 'text',
                        emails: {
                            type: {
                                list: 'text'
                            }
                        }
                    }, {
                        primaryKeys: ['name']
                    });
                });
            });
            it ('should be able to create a schema using map types', () => {
                assert.doesNotThrow(() => {
                    new Cassandra.Schema({
                        name: 'text',
                        emails: {
                            type: {
                                map: ['timestamp', 'text']
                            }
                        }
                    }, {
                        primaryKeys: ['name']
                    });
                });
            });
            it ('should be able to create a schema using set types', () => {
                assert.doesNotThrow(() => {
                    new Cassandra.Schema({
                        name: 'text',
                        emails: {
                            type: {
                                set: 'text'
                            }
                        }
                    }, {
                        primaryKeys: ['name']
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
            testCompositeModel,
            testListModel,
            testMapModel,
            testSetModel,
            testList,
            testMap,
            testSet;
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
            testList = new Cassandra.Schema({
                usernames: {
                    type: {
                        list: 'text'
                    }
                },
                name: 'text',
                age: 'int'
            }, {
                primaryKeys: ['name'],
                indexes: ['usernames']
            });
            testSet = new Cassandra.Schema({
                usernames: {
                    type: {
                        set: 'text'
                    }
                },
                name: 'text',
                age: 'int'
            }, {
                primaryKeys: ['name'],
                indexes: ['usernames']
            });
            testMap = new Cassandra.Schema({
                usernames: {
                    type: {
                        map: ['timestamp', 'text']
                    }
                },
                name: 'text',
                age: 'int'
            }, {
                primaryKeys: ['name'],
                indexes: ['usernames']
            });
        });
        after(() => {
            //map all the qualified names to a drop table function
            async.parallel([
                testPartitionModel.qualifiedName,
                testCompositeModel.qualifiedName,
                testCompoundModel.qualifiedName,
                testListModel.qualifiedName,
                testSetModel.qualifiedName,
                testMapModel.qualifiedName
            ].map((tableName) => {
                return (next) => cassandra.driver.execute(format('DROP TABLE %s.%s', cassandra.keyspace, tableName), next);
            }), done);
        });
        it.skip ('should be able to create a table with table options object', () => {
        });
        it ('should fail at creating a Model without a Cassandra instance(db)', () => {
            assert.throws(() => {
                new Cassandra.Model();
            }, (err) => {
                return err instanceof TypeError
                    && err.message === 'Model expects parameter 1 to be an instanceof Cassandra';
            });
        });
        it ('should fail at creating a Model without a name', () => {
            assert.throws(() => {
                new Cassandra.Model(cassandra);
            }, (err) => {
                return err instanceof TypeError
                    && err.message === 'Model expects parameter 2 to be of type "string": @"undefined"';
            });
        });
        it ('should fail at creating a Model without a valid schema', () => {
            assert.throws(() => {
                new Cassandra.Model(cassandra, 'fail', {});
            }, (err) => {
                return err instanceof TypeError
                    && err.message === 'Model expects parameter 3 to be an instance of Cassandra.Schema: "undefined"';
            });
        });
        it ('should be able to attach a model to the db instance', (done) => {
            async.parallel([
                (next) => testPartitionModel = cassandra.model('testpartition', testPartition, next),
                (next) => testCompoundModel = cassandra.model('testcompound', testCompound, next),
                (next) => testCompositeModel = cassandra.model('testcomposite', testComposite, next)
            ], done);
        });
        it ('should be able to attach a model to the db instance and create indexes on it', (done) => {
            async.parallel([
                (next) => testListModel = cassandra.model('testlist', testList, next),
                (next) => testSetModel = cassandra.model('testset', testSet, next),
                (next) => testMapModel = cassandra.model('testmap', testMap, next)
            ], (err) => {
                if (err) {
                    return done(err);
                }
                assert.equal(testListModel.model.indexes.usernames, 'usernames');
                assert.equal(testSetModel.model.indexes.usernames, 'usernames');
                assert.equal(testMapModel.model.indexes.usernames, 'usernames');
                done();
            });
        });
        it ('should fail at buiding the schema after instantiation', () => {
            assert.throws(() => testPartitionModel.model._buildSchema());
        });
        it ('should not qualify a query object if it has foreign properties', () => {
            assert.throws(() => {
                testPartitionModel.model._qualifyQueryColumns({foo: 'bar'});
            }, (err) => {
                return err instanceof Error
                    && err.message === 'Not a valid queryObject, could not find column: foo, model: testpartition';
            });
        });
        it ('should not qualify a query object if it has foreign properties', () => {
            assert.throws(() => {
                testPartitionModel.model._buildQueryComponents({
                    username: {
                        $badOperator: 'bar'
                    }
                });
            }, (err) => {
                return err instanceof Error
                    && err.message === 'Invalid Operator type, not supported: $badOperator';
            });
        });
        it ('should properly build queries with contains/contains key', () => {
            assert.doesNotThrow(() => {
                testPartitionModel.model._buildQueryComponents({
                    username: {
                        $contains: 'bar'
                    }
                });
            });
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
                    name: 'text',
                    testlist: {
                        type: {
                            list: 'text'
                        }
                    },
                    testmap: {
                        type: {
                            map: ['text', 'int']
                        }
                    },
                    testset: {
                        type: {
                            set: 'text'
                        }
                    },
                    mappedKey: 'text' //mapped for case-sensitivity
                }, {
                    indexes: ['testlist', 'testmap', 'testset'],
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
                before((done) => {
                    async.parallel([
                        (next) => testListModel = cassandra.model('testlist', testList, next),
                        (next) => testSetModel = cassandra.model('testset', testSet, next),
                        (next) => testMapModel = cassandra.model('testmap', testMap, next)
                    ], done);
                });
                it ('should be able to perform a basic insert', (done) => {
                    async.parallel([
                        (next) => TestModel.insert({
                            username: 'foo',
                            age: 30,
                            name: 'bar',
                            testlist: ['foo'],
                            testset: ['foo'],
                            testmap: {foo: 3}
                        }, next),
                        (next) => TestModel.insert({username: 'foo', age: 31, name: 'bazz'}, next),
                        (next) => TestModel.insert({username: 'baz', age: 32, name: 'bars'}, next)
                    ], (err) => {
                        done(err);
                    });
                });
                it ('should be able to insert list types', (done) => {
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testListModel.findOne({name: 'ListType', usernames: {$contains: 'baz'}}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.deepEqual(row.usernames, ['baz', 'bars']);
                            done();
                        });
                    };
                    testListModel.insert({usernames: ['baz', 'bars'], age: 32, name: 'ListType'}, check);
                });
                it ('should be able to insert set types', (done) => {
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testSetModel.findOne({name: 'SetType', usernames: {$contains: 'baz'}}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.deepEqual(row.usernames, ['bars', 'baz']);
                            done();
                        });
                    };
                    testSetModel.insert({usernames: ['baz', 'bars'], age: 32, name: 'SetType'}, check);
                });
                it ('should be able to insert map types and find with "$containsKey"', (done) => {
                    var userMap = {};
                    var userMapKey = Date();
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testMapModel.findOne({name: 'MapType', usernames: {$containsKey: userMapKey}}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert(row.usernames[userMapKey]);
                            assert.equal(row.usernames[userMapKey], 'fii');
                            done();
                        });
                    };
                    userMap[userMapKey] = 'fii';
                    testMapModel.insert({usernames: userMap, age: 32, name: 'MapType'}, check);
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
                it ('should pass null to callback if nothing found', (done) => {
                    TestModel.findOne({username: 'no one'}, (err, row) => {
                        if (err) {
                            done(err);
                        } else {
                            assert.equal(row, null);
                            done();
                        }
                    });
                });
                it ('should be able to pass options to find', (done) => {
                    TestModel.findOne({username: 'foo'}, {allowFiltering: true}, (err, row) => {
                        if (err) {
                            done(err);
                        } else {
                            assert.equal(row.username, 'foo');
                            done();
                        }
                    });
                });
            });

            describe('Find >', () => {//{{{
                it ('should throw an error trying to perform a find without a callback', () => {
                    assert.throws(() => TestModel.find({username: 'foo', age: 30}));
                });
                it ('should be able to perform a basic find with queryObject', (done) => {
                    TestModel.find({username: 'foo', age: 30}, (err, rows) => {
                        if (err) {
                            return done(err);
                        }
                        assert.equal(rows.length, 1);
                        assert(rows[0] instanceof TestModel);
                        assert.equal(rows[0].username, 'foo');
                        assert.equal(rows[0].age, 30);
                        done();
                    });
                });
                it ('should be able to perform a basic find all with an empty queryObject', (done) => {
                    TestModel.find({}, (err, rows) => {
                        if (err) {
                            return done(err);
                        }
                        assert.equal(rows.length, 3);
                        done();
                    });
                });
                it ('should be able to perform a basic find without a queryObject', (done) => {
                    TestModel.find((err, rows) => {
                        if (err) {
                            return done(err);
                        }
                        assert.equal(rows.length, 3);
                        done();
                    });
                });
                it ('should error with invalid params', (done) => {
                    TestModel.find.apply(TestModel, [{username: 'foo', age: 30}, ['fii'], (err, rows) => {
                        if (err) {
                            return done();
                        }
                        done(new Error('Should have returned an error'));
                    }]);
                });
                it ('should error with invalid projection', (done) => {
                    TestModel.find.apply(TestModel, [{username: 'foo', age: 30}, ['fii','fox'], (err, rows) => {
                        if (err) {
                            return done();
                        }
                        done(new Error('Should have returned an error'));
                    }]);
                });
                it ('should maintain a mapping of case-sensitive fields', (done) => {
                    TestModel.find({username: 'foo', age: 30}, (err, rows) => {
                        if (err) {
                            return done(err);
                        }
                        assert.equal(rows.length, 1);
                        assert(rows[0] instanceof TestModel);
                        assert.equal(rows[0].username, 'foo');
                        assert.equal(rows[0].age, 30);
                        assert.deepEqual(Object.keys(rows[0]), ['age', 'mappedKey', 'name', 'testlist', 'testmap', 'testset', 'username'], 'did not use the right order/likely wrong column family used');
                        done();
                    });
                });
                it ('should maintain a mapping of case-sensitive fields with raw option', (done) => {
                    TestModel.find({username: 'foo', age: 30}, {raw: true}, (err, rows) => {
                        if (err) {
                            return done(err);
                        }
                        assert.equal(rows.length, 1);
                        assert.equal(rows[0].username, 'foo');
                        assert.equal(rows[0].age, 30);
                        assert.deepEqual(Object.keys(rows[0]), ['username', 'age', 'name', 'testlist', 'testmap', 'testset', 'mappedKey'], 'did not use the right order/likely wrong column family used');
                        done();
                    });
                });
                it ('should be able to perform a basic find with queryObject and return raw rows', (done) => {
                    TestModel.find({username: 'foo', age: 30}, {raw: true}, (err, rows) => {
                        if (err) {
                            return done(err);
                        }
                        assert.equal(rows.length, 1);
                        assert(! (rows[0] instanceof TestModel));
                        assert.equal(rows[0].username, 'foo');
                        assert.equal(rows[0].age, 30);
                        done();
                    });
                });
                it ('should be able to perform a basic find that ALLOWS FILTERING', (done) => {
                    TestModel.find({username: 'foo', age: 30}, {allowFiltering: true}, (err, rows) => {
                        if (err) {
                            return done(err);
                        }
                        assert.equal(rows.length, 1);
                        assert.equal(rows[0].username, 'foo');
                        assert.equal(rows[0].age, 30);
                        done();
                    });
                });
                it ('should be able to perform a complex find with queryObject and operators', (done) => {
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
                it ('should be able to perform a complex find with queryObject and filters', (done) => {
                    TestModel.find({
                        username: 'foo',
                        age: {
                            $in: [29, 30, 31]
                        }
                    }, ['username', 'age'], (err, rows) => {
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
                it ('should be able to perform a complex find and filter limit', (done) => {
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
                it ('should be able to find list types', (done) => {
                    TestModel.find({
                        testlist: {$contains: 'foo'}
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
                it ('should be able to find map types', (done) => {
                    TestModel.find({
                        testmap: {$containsKey: 'foo'}
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
                it ('should be able to find set types', (done) => {
                    TestModel.find({
                        testset: {$contains: 'foo'}
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
                it ('should be able to perform an update on a multiple values', (done) => {
                    TestModel.update({username: 'foo', age: 30}, {name: 'test3', mappedKey: 'foo'}, (err, rows) => {
                        done(err);
                    });
                });
                it ('should be able to update list types by changing the entire column', (done) => {
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testListModel.findOne({name: 'ListType', usernames: {$contains: 'bar'}}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.deepEqual(row.usernames, ['bar']);
                            done();
                        });
                    };
                    testListModel.update({
                        name: 'ListType'
                    }, {
                        usernames: ['bar']
                    }, check);
                });
                it ('should be able to update list types by appending items', (done) => {
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testListModel.findOne({name: 'ListType', usernames: {$contains: 'foo'}}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.equal(row.name, 'ListType');
                            assert.deepEqual(row.usernames, ['bar', 'foo']);
                            done();
                        });
                    };
                    testListModel.update({
                        name: 'ListType'
                    }, {
                        usernames: {
                            $append: ['foo']
                        }
                    }, check);
                });
                it ('should be able to update list types by appending multiple items', (done) => {
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testListModel.findOne({name: 'ListType', usernames: {$contains: 'yee'}}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.equal(row.name, 'ListType');
                            assert.deepEqual(row.usernames, ['bar', 'foo', 'yee', 'haw']);
                            done();
                        });
                    };
                    testListModel.update({
                        name: 'ListType'
                    }, {
                        usernames: {
                            $append: ['yee', 'haw']
                        }
                    }, check);
                });
                it ('should be able to update list types by prepending items', (done) => {
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testListModel.findOne({name: 'ListType', usernames: {$contains: 'fii'}}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.equal(row.name, 'ListType');
                            assert.deepEqual(row.usernames, ['fii', 'bar', 'foo', 'yee', 'haw']);
                            done();
                        });
                    };
                    testListModel.update({
                        name: 'ListType'
                    }, {
                        usernames: {
                            $prepend: ['fii']
                        }
                    }, check);
                });
                it ('should be able to update list types by prepending multiple items', (done) => {
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testListModel.findOne({name: 'ListType', usernames: {$contains: 'foi'}}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.equal(row.name, 'ListType');
                            assert.deepEqual(row.usernames, ['foi', 'fui', 'fii', 'bar', 'foo', 'yee', 'haw']);
                            done();
                        });
                    };
                    testListModel.update({
                        name: 'ListType'
                    }, {
                        usernames: {
                            $prepend: ['foi', 'fui']
                        }
                    }, check);
                });
                it ('should be able to update list types by prepending and appending items', (done) => {
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testListModel.findOne({name: 'ListType', usernames: {$contains: 'cho'}}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.equal(row.name, 'ListType');
                            assert.deepEqual(row.usernames, ['cho', 'foi', 'fui', 'fii', 'bar', 'foo', 'yee', 'haw', 'cow']);
                            done();
                        });
                    };
                    testListModel.update({
                        name: 'ListType'
                    }, {
                        usernames: {
                            $append: ['cow'],
                            $prepend: ['cho']
                        }
                    }, check);
                });
                it ('should be able to update list types by filtering items', (done) => {
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testListModel.findOne({name: 'ListType', usernames: {$contains: 'bar'}}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.equal(row.name, 'ListType');
                            assert.deepEqual(row.usernames, ['bar', 'foo']);
                            done();
                        });
                    };
                    testListModel.update({
                        name: 'ListType'
                    }, {
                        usernames: {
                            $filter: ['cho', 'foi', 'fui', 'fii', 'cow', 'yee', 'haw']
                        }
                    }, check);
                });
                it ('should be able to update list types by using an index', (done) => {
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testListModel.findOne({name: 'ListType', usernames: {$contains: 'foo'}}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.equal(row.name, 'ListType');
                            assert.deepEqual(row.usernames, ['foo', 'baz']);
                            done();
                        });
                    };
                    testListModel.update({
                        name: 'ListType'
                    }, {
                        usernames: {
                            $set: {
                                0: 'foo',
                                1: 'baz'
                            }
                        }
                    }, check);
                });
                it ('should be able to add new values to existing sets', (done) => {
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testSetModel.findOne({name: 'SetType', usernames: {$contains: 'aba'}}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.deepEqual(row.usernames, ['aba', 'bars', 'baz']);
                            done();
                        });
                    };
                    testSetModel.update({
                        name: 'SetType'
                    }, {
                        usernames: {
                            $add: ['aba']
                        }
                    }, check);
                });
                it ('should be able to remove values from existing sets', (done) => {
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testSetModel.findOne({name: 'SetType', usernames: {$contains: 'aba'}}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.deepEqual(row.usernames, ['aba', 'baz']);
                            done();
                        });
                    };
                    testSetModel.update({
                        name: 'SetType'
                    }, {
                        usernames: {
                            $filter: ['bars']
                        }
                    }, check);
                });
                it ('should be able to update a set to empty', (done) => {
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testSetModel.findOne({name: 'SetType'}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.deepEqual(row.usernames, null);
                            done();
                        });
                    };
                    testSetModel.update({
                        name: 'SetType'
                    }, {
                        usernames: []
                    }, check);
                });
                it ('should be able to update a map value to null', (done) => {
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testMapModel.findOne({name: 'MapType'}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.deepEqual(row.usernames, null);
                            done();
                        });
                    };
                    testMapModel.update({
                        name: 'MapType'
                    }, {
                        usernames: {
                            $set: {}
                        }
                    }, check);
                });

                it ('should be able to update a map value', (done) => {
                    var mapKey = Date();
                    var mapObject = {};
                    var mapChange = {};
                    mapObject[mapKey] = 'foo';
                    mapChange[mapKey] = 'fii';
                    var check = (err) => {
                        testMapModel.findOne({
                            name: 'MapType'
                        }, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert(!!row);
                            assert.equal(row.usernames[mapKey], 'fii');
                            done();
                        });
                    };
                    var next = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testMapModel.update({
                            name: 'MapType'
                        }, {
                            usernames: mapChange
                        }, check);
                    };
                    testMapModel.update({
                        name: 'MapType'
                    }, {
                        usernames: mapObject
                    }, next);
                });

                it ('should be able to add a new map value', (done) => {
                    var mapKey = Date();
                    var secondKey = new Date(0);
                    var mapObject = {};
                    mapObject[mapKey] = 'foo';
                    mapObject[secondKey] = 'baz';
                    var check = (err) => {
                        testMapModel.findOne({
                            name: 'MapType'
                        }, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert(!!row);
                            assert.equal(row.usernames[mapKey], 'foo');
                            done();
                        });
                    };
                    testMapModel.update({
                        name: 'MapType'
                    }, {
                        usernames: {$set: mapObject}
                    }, check);
                });
                it ('should be able to add a new item and update an existing', (done) => {
                    var mapKey = new Date(1E6);
                    var secondKey = new Date(0);
                    var mapObject = {};
                    mapObject[mapKey] = 'fox';
                    mapObject[secondKey] = 'bar';
                    var check = (err) => {
                        testMapModel.findOne({
                            name: 'MapType'
                        }, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert(!!row);
                            assert.equal(row.usernames[mapKey], 'fox');
                            assert.equal(row.usernames[secondKey], 'bar');
                            done();
                        });
                    };
                    testMapModel.update({
                        name: 'MapType'
                    }, {
                        usernames: mapObject
                    }, check);
                });
            });

            describe('Delete >', () => {//{{{
                it ('should throw an error trying to perform a delete without a callback', () => {
                    assert.throws(() => TestModel.delete({username: 'foo', age: 30}));
                });

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
                it ('should not find any rows to delete USING TIMESTAMP older than now', (done) => {
                    var query = {
                            username: 'foo',
                            age: 31
                        },
                        options = {
                            $usingTimestamp: (Date.now() - 10000) * 1000
                        };
                    TestModel.delete(query, options, (err, result) => {
                        if (err) {
                            done(err);
                        } else {
                            TestModel.find(query, (err, result) => {
                                if (err) {
                                    done(err);
                                } else {
                                    assert.equal(result.length, 1);
                                    done();
                                }
                            });
                        }
                    });
                });
                it ('should be able to delete row(s) by USING TIMESTAMP', (done) => {
                    var query = {
                            username: 'foo',
                            age: 31
                        },
                        options = {
                            $usingTimestamp: (Date.now() + 10000) * 1000
                        };
                    TestModel.delete(query, options, (err, result) => {
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
                it ('should be able to delete list types by index', (done) => {
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testListModel.findOne({name: 'ListType'}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.deepEqual(row.usernames, ['foo']);
                            done();
                        });
                    };
                    testListModel.delete({
                        name: 'ListType'
                    }, {
                        usernames: [1] //remove second index
                    }, check);
                });
                it ('should be able to delete list types by index and column', (done) => {
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testListModel.findOne({name: 'ListType'}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.equal(row.age, null);
                            assert.equal(row.usernames, null);
                            testListModel.insert({usernames: ['baz', 'bars', 'foo', 'fii'], age: 32, name: 'ListType'}, done);
                        });
                    };
                    testListModel.delete({
                        name: 'ListType'
                    }, {
                        age: 1,
                        usernames: [0] //remove first index
                    }, check);
                });
                it ('should be able to delete list types with multiple indexes', (done) => {
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testListModel.findOne({name: 'ListType'}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.equal(row.age, null);
                            assert.deepEqual(row.usernames, ['bars', 'fii']);
                            done();
                        });
                    };
                    testListModel.delete({
                        name: 'ListType'
                    }, {
                        age: 1,
                        usernames: [0, 2] //remove first and third index
                    }, check);
                });
                it ('should be able to delete set types', (done) => {
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testSetModel.findOne({name: 'SetType'}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.deepEqual(row.usernames, null);
                            done();
                        });
                    };
                    testSetModel.delete({
                        name: 'SetType'
                    }, {
                        usernames: 1
                    }, check);
                });
                it ('should be able to delete map types by their keys', (done) => {
                    var row = null;
                    var newMap = {};
                    var newMapKey = Date();
                    var secondKey = new Date(0);
                    newMap[newMapKey] = 'hello world';
                    newMap[secondKey] = 'foo';
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testMapModel.findOne({name: 'MapType'}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert(!!row);
                            assert(!!row.usernames);
                            assert(!!row.usernames[secondKey]);
                            assert(!row.usernames[newMapKey]);
                            assert.equal(row.usernames[secondKey], 'foo');
                            done();
                        });
                    };
                    var test = () => {
                        testMapModel.delete({
                            name: 'MapType'
                        }, {
                            usernames: [newMapKey]
                        }, check);
                    };
                    var find = () => {
                        testMapModel.findOne({name: 'MapType'}, (err, result) => {
                            if (err) {
                                return done(err);
                            }
                            row = result;
                            test();
                        });
                    };
                    testMapModel.update({name: 'MapType'}, {usernames: {$set: newMap}}, find);
                });
                it ('should be able to delete map types', (done) => {
                    var check = (err) => {
                        if (err) {
                            return done(err);
                        }
                        testMapModel.findOne({name: 'MapType'}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.deepEqual(row.usernames, null);
                            done();
                        });
                    };
                    testMapModel.delete({
                        name: 'MapType'
                    }, {
                        usernames: 1
                    }, check);
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
        it ('should expose model, views, and a name property', () => {
            var User = new UserModel({
                    hex: Cassandra.uuid(),
                    names: 'baz',
                    username: 'fii'
                });
            assert(User.model instanceof Cassandra.Model, 'Model is not bound properly');
            assert(User.model.Factory.model instanceof Cassandra.Model, 'Model should be a circular reference');
            assert(!!User.views, 'Model.views is not bound properly');
            assert(User.model.name, 'Model.name is not bound properly');
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
            it ('should be able to set null values', () => {
                var user = new UserModel({
                        hex: Cassandra.uuid(),
                        names: 'baz',
                        username: null
                    });
                assert.equal(user.username, null);
                assert.equal(user.age, 50, 'did not properly set default age value');
            });
        });

        describe('Methods >', () => {
            describe('Sync >', () => {
                it ('should only sync fields that exist in the schema model', (done) => {
                    var user = new UserModel({
                            hex: Cassandra.uuid(),
                            names: 'baz',
                            username: 'fii'
                        });
                    user.age = 35;
                    user.badProp = 'should not exist';
                    user.sync((err) => {
                        assert.equal(user.age, 35, 'did not properly set default age value');
                        assert.equal(user.age, user.__$$object.age);
                        done();
                    });
                });
                it ('should be able to sync a new model instance', (done) => {
                    var user = new UserModel({
                            hex: Cassandra.uuid(),
                            names: 'baz31',
                            username: 'fii31'
                        });
                    user.sync((err) => {
                        done(err);
                    });
                });
                it ('should be able to sync a new model instance with null values', (done) => {
                    var user = new UserModel({
                            hex: Cassandra.uuid(),
                            names: 'baz32',
                            username: null
                        });
                    user.sync((err) => {
                        if (err) {
                            return done(err);
                        }
                        UserModel.findOne({names: 'baz32'}, (err, row) => {
                            if (err) {
                                return done(err);
                            }
                            assert.equal(row.username, null);
                            assert.equal(row.names, 'baz32');
                            done();
                        });
                    });
                });
            });

            describe('Save >', () => {
                it ('should fail at saving a primary key change', (done) => {
                    var user = new UserModel({
                            hex: Cassandra.uuid(),
                            names: 'primTest',
                            username: 'primTestUserName'
                        });
                    user.save((err) => {
                        if (err) {
                            return done(err);
                        }
                        user.names = 'faa';
                        user.save((err) => {
                            assert(err instanceof Error);
                            assert.equal(err.message, 'PRIMARY KEY part names found in SET part');
                            done();
                        });
                    });
                });
                it ('should properly update the data model following a save', (done) => {
                    var user = new UserModel({
                            hex: Cassandra.uuid(),
                            names: 'baz3',
                            username: 'fii3'
                        });
                    user.save((err) => {
                        if (err) {
                            return done(err);
                        }
                        user.username = 'fii4';
                        user.save((err) => {
                            assert.equal(user.username, 'fii4');
                            assert.equal(user.username, user.__$$object.username);
                            done();
                        });
                    });
                });
                it ('should be able to save null values', (done) => {
                    var user = new UserModel({
                            hex: Cassandra.uuid(),
                            names: 'baz40',
                            username: 'fii40'
                        }),
                        check = (err) => {
                            if (err) {
                                return done(err);
                            }
                            UserModel.findOne({names: 'baz40'}, {raw: true}, (err, row) => {
                                if (err) {
                                    return done(err);
                                }
                                assert.equal(row.username, null);
                                assert.equal(row.names, 'baz40');
                                done();
                            });
                        };
                    user.save((err) => {
                        if (err) {
                            return done(err);
                        }
                        user.username = null;
                        user.save(check);
                    });
                });
            });

            describe('Restore >', () => {
                it ('should be able to restore data model from failed attempts', (done) => {
                    var user = new UserModel({
                            hex: Cassandra.uuid(),
                            names: 'primTest2',
                            username: 'primTestUserName2'
                        });
                    user.save((err) => {
                        if (err) {
                            return done(err);
                        }
                        user.names = 'faa';
                        user.save((err) => {
                            assert(err instanceof Error);
                            assert.equal(err.message, 'PRIMARY KEY part names found in SET part');
                            assert.equal(user.names, 'faa');
                            user.restore();
                            assert.equal(user.names, 'primTest2');
                            done();
                        });
                    });
                });
            });

            describe('Delete >', () => {
                it('should be able to delete a column from the instance\'s column family', (done) => {
                    var user = new UserModel({
                            hex: Cassandra.uuid(),
                            names: 'primTest2',
                            username: 'primTestUserName2'
                        });
                    user.save((err) => {
                        if (err) {
                            return done(err);
                        }
                        user.delete({username: 1}, (err) => {
                            assert(!err, err);
                            UserModel.findOne({names: 'primTest2'}, (err, row) => {
                                if (err) {
                                    return done(err);
                                }
                                assert.equal(row.names, 'primTest2');
                                assert.equal(row.username, null);
                                done();
                            });
                        });
                    });
                });
                it('should be able to delete an entire row from a table', (done) => {
                    var user = new UserModel({
                            hex: Cassandra.uuid(),
                            names: 'primTest2',
                            username: 'primTestUserName2'
                        });
                    user.save((err) => {
                        if (err) {
                            return done(err);
                        }
                        user.delete((err) => {
                            assert(!err, err);
                            UserModel.findOne({names: 'primTest2'}, (err, row) => {
                                if (err) {
                                    return done(err);
                                }
                                assert.equal(row, null);
                                done();
                            });
                        });
                    });
                });
            });
        });
    });

    describe('Indexes >', () => {
        var indexSchema, IndexModel;
        before((done) => {
            indexSchema = new Cassandra.Schema({
                name: 'text',
                username: 'text'
            }, {
                primaryKeys: ['name']
            });
            IndexModel = cassandra.model('testmodelindexes', indexSchema, done);
        });
        it ('should be able to create indexes after the model has been instantiated', (done) => {
            IndexModel.model.createIndex('username', done);
        });
        it ('should throw an error if an invalid index is specified', () => {
            assert.throws(() => {
                IndexModel.model.createIndex('fail');
            }, (err) => {
                if (!err) {
                    return done(new Error('should have thrown an error'));
                }
                return err instanceof Error
                    && err.message === 'Invalid Index, could not find column "fail" in table model "testmodelindexes"';
            });
        });
        it ('should throw an error if an invalid index mapping is specified', () => {
            assert.throws(() => {
                IndexModel.model.createIndex({fail: 'fail'});
            }, (err) => {
                if (!err) {
                    return done(new Error('should have thrown an error'));
                }
                return err instanceof Error
                    && err.message === 'Invalid Index, could not find column "fail" in table model "testmodelindexes"';
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
        //delete views and drop column family
        after((done) => {
            async.series([
                (next) => async.each([
                        TestModel.views.byAge.qualifiedName,
                        TestModel.views.byName.qualifiedName,
                        TestModel.views.byNameAndAge.qualifiedName
                    ], (viewName, cb) => {
                        cassandra.driver.execute(format(
                            'DROP MATERIALIZED VIEW %s.%s',
                            cassandra.keyspace,
                            viewName), cb);
                    }, next),
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
                var query = 'SELECT view_name FROM system_schema.views WHERE keyspace_name = \''
                        + cassandra.keyspace + "' and view_name in ("
                        + Object.keys(TestModel.views).map((key) => "'" + TestModel.views[key].qualifiedName + "'").join(',')
                        + ')';
                cassandra.driver.execute(query, (err, result) => {
                    if (err) {
                        return done(err);
                    }
                    var rows = result.rows;
                    assert.equal(rows.length, 2);
                    assert.equal(rows[0].view_name, TestModel.views.byAge.qualifiedName);
                    assert.equal(rows[1].view_name, TestModel.views.byName.qualifiedName);
                    done();
                });
            });
        });
        it ('should throw an error trying to attach the same view twice', () => {
            assert.throws(() => TestModel.model.createView('byName', {}, null, null));
        });
        it ('should throw an error if there is not at least a primaryKey property', () => {
            assert.throws(() => {
                TestModel.model.createView('noPrimary', {});
            }, (err) => {
                return err instanceof Error
                    && err.message === 'Views must specify at least a "primaryKeys" '
                        +'property to create from table; view: noPrimary';
            });
        });
        it ('should throw an error if the primary keys do not qualify', () => {
            assert.throws(() => {
                new Cassandra.Schema({
                    name: 'text'
                }, {
                    primaryKeys: ['badKey']
                });
            }, (err) => {
                return err instanceof Error
                    && err.message === 'Invalid Primary Key, column not found in schema model; @key: badKey';
            });
        });
        it ('should throw an error if the view does not qualify', () => {
            assert.throws(() => TestModel.model.createView('byNames', {primaryKeys:['foo']}, null, null));
        });
        it ('should return error if failing to build view', (done) => {
            TestModel.model.createView('failTest', {
                select: ['name', 'age', 'username'],
                primaryKeys: ['name', 'age']
            }, (err) => {
                if (err) {
                    return done();
                }
                done(new Error('Should have thrown an error'));
            });
        });
        it ('should return error if trying to build an existing view with a callback', (done) => {
            TestModel.model.createView('byName', {
                primaryKeys: ['name']
            }, (err) => {
                if (err) {
                    return done();
                }
                done(new Error('Should have thrown an error'));
            });
        });
        it ('should return query string to build view without callback', () => {
            var query = TestModel.model.createView('stringTest', {
                    select: ['name', 'age', 'username'],
                    primaryKeys: ['name','username','age']
                }, false);
            var match = 'CREATE MATERIALIZED VIEW IF NOT EXISTS '
                + 'testkeyspace.testschema__stringtest AS SELECT '
                + 'name, age, username FROM testschema WHERE name '
                + 'IS NOT NULL AND username IS NOT NULL AND age IS '
                + 'NOT NULL PRIMARY KEY (name, username, age)';
            assert.equal(query, match, 'Queries do not match');
        });
        it ('should throw an error if failing to build view without callback', () => {
            assert.throws(() => {
                TestModel.model.createView('byName', {
                    select: ['name', 'age', 'username'],
                    primaryKeys: ['name']
                });
            });
        });
        it ('should be able to create a view after instantiation', (done) => {
            TestModel.model.createView('byNameAndAge', {
                select: ['name'],
                primaryKeys: ['name']
            }, () => {
                cassandra.driver.execute(
                    'SELECT view_name FROM system_schema.views WHERE '
                    + 'keyspace_name=? AND view_name=?',
                    [cassandra.keyspace, TestModel.views.byNameAndAge.qualifiedName],
                    {prepare: true},
                    (err, result) => {
                        if (err) {
                            return done(err);
                        }
                        var rows = result.rows;
                        assert.equal(rows.length, 1);
                        done();
                    }
                );
            });
        });

        describe('Find >', () => {
            before((done) => {
                async.parallel([
                    (next) => TestModel.insert({username: 'foo', age: 30, name: 'bar'}, next),
                    (next) => TestModel.insert({username: 'foos', age: 32, name: 'baz'}, next)
                ], done);
            });

            it ('should be able to find rows specific to the materialized view and return an array', (done) => {
                TestModel.views.byName.find({name: 'bar'}, (err, row) => {
                    if (err) {
                        done(err);
                    } else {
                        assert.equal(row.length, 1, 'row result is not an array');
                        assert.equal(row[0].name, 'bar', 'did not find the right row');
                        assert.equal(Object.keys(row[0])[0], 'age', 'did not use the right order/likely wrong column family used');
                        done();
                    }
                });
            });
            it ('should be able to find rows specific to a projection and return an array', (done) => {
                TestModel.views.byName.find({name: 'bar'}, ['name'], (err, row) => {
                    if (err) {
                        done(err);
                    } else {
                        assert.equal(row.length, 1, 'row result is not an array');
                        assert.equal(row[0].name, 'bar', 'did not find the right row');
                        assert.equal(Object.keys(row[0])[0], 'age', 'did not use the right order/likely wrong column family used');
                        done();
                    }
                });
            });
            it ('should be able to perform a basic find all with an empty queryObject', (done) => {
                TestModel.views.byName.find({}, (err, rows) => {
                    if (err) {
                        return done(err);
                    }
                    assert.equal(rows.length, 2);
                    done();
                });
            });
            it ('should be able to perform a basic find without a queryObject', (done) => {
                TestModel.views.byName.find((err, rows) => {
                    if (err) {
                        return done(err);
                    }
                    assert.equal(rows.length, 2);
                    done();
                });
            });
        });

        describe('FindOne >', () => {
            before((done) => {
                TestModel.insert({username: 'foo', age: 30, name: 'bar'}, done);
            });

            it ('should properly handle arguments even when undefined is passed', (done) => {
                TestModel.findOne.apply(TestModel, [{username: 'foo'}, (err, row) => {
                    if (err) {
                        done(err);
                    } else {
                        assert.equal( ! Array.isArray(row), 1, 'row result should not be an array');
                        assert.equal(row.username, 'foo', 'did not find the right row');
                        assert.equal(Object.keys(row)[0], 'age', 'did not use the right order/likely wrong column family used');
                        done();
                    }
                }, undefined]);
            });
            it ('should be able to find a single row and return an object', (done) => {
                TestModel.views.byName.findOne({name: 'bar'}, (err, row) => {
                    if (err) {
                        done(err);
                    } else {
                        assert.equal( ! Array.isArray(row), 1, 'row result should not be an array');
                        assert.equal(row.name, 'bar', 'did not find the right row');
                        assert.equal(Object.keys(row)[0], 'age', 'did not use the right order/likely wrong column family used');
                        done();
                    }
                });
            });
            it ('should be able to find a single row specific to a projection and return an array', (done) => {
                TestModel.views.byName.findOne({name: 'bar'}, ['name'], (err, row) => {
                    if (err) {
                        done(err);
                    } else {
                        assert.equal( ! Array.isArray(row), 1, 'row result should not be an array');
                        assert.equal(row.name, 'bar', 'did not find the right row');
                        assert.equal(Object.keys(row)[0], 'age', 'did not use the right order/likely wrong column family used');
                        done();
                    }
                });
            });
        });
    });//}}}

});
