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

describe('Cassandra', function (done) {
    before((done) => {
        cassandra = Cassandra.connect(config);
        cassandra.on('error', done);
        cassandra.on('connect', done);
    });

    describe('Keyspace', () => {
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

    describe('Schema', () => {
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

    describe('Model', () => {
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
                (next) => cassandra.driver.execute(format('DROP TABLE %s.%s', cassandra.keyspace, testPartitionModel.name), next),
                (next) => cassandra.driver.execute(format('DROP TABLE %s.%s', cassandra.keyspace, testCompositeModel.name), next),
                (next) => cassandra.driver.execute(format('DROP TABLE %s.%s', cassandra.keyspace, testCompoundModel.name), next)
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
    });

    describe('Views', () => {
        var testSchema, testModel;
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
                        testModel.name + '__byname'), next),
                (next) => cassandra.driver.execute(format(
                        'DROP MATERIALIZED VIEW %s.%s', 
                        cassandra.keyspace, 
                        testModel.name + '__byage'), next),
                (next) => cassandra.driver.execute(format(
                        'DROP TABLE %s.%s', 
                        cassandra.keyspace, 
                        testModel.name), next)
            ], done);
        });
        it ('should be able to create views and attach them to the model', (done) => {
            testModel = cassandra.model('testschema', testSchema, (err, result) => {
                assert(!err, err);
                setTimeout(() => {
                    cassandra.driver.execute('SELECT * FROM system.built_views WHERE keyspace_name = \''
                            + cassandra.keyspace + '\'', (err, row) => {
                        assert(!err, err);
                        assert.equal(row.rowLength, 2);
                        done();
                    });
                }, 200);
            });
        });
    });
        
    
    describe.skip('Insert', () => {
        var testSchema, testModel;
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
                    }
                }
            });
            testModel = cassandra.model('testschema', testSchema);
        });
        it ('should be able to perform a basic insert', (done) => {
            testModel.insert({username: 'foo', age: 30, name: 'bar'}, (err, result) => {
                console.log(999, err, result);
            });
        });
    });


    it.skip ('should be able to attach static methods', () => {
    });
    it.skip ('should convert uuid types from strings to objects or fail', () => {
    });
    it.skip ('should be able to create a table with table options object', () => {
    });
});
