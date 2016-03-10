const assert = require('assert');
const Cassandra = require('../');
const keyspaceName = 'testkeyspace';
const async = require('async');
const keyspaceConfig = {
    'with replication': {
        class: 'SimpleStrategy',
        replication_factor: 1
    },
    durable_writes: true
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
        it ('should create a Schema object', () => {
            //var type = cassandra.types;
            var userSchema = new Cassandra.Schema({
                    username: 'text',
                    age: 'int'
                }, {
                    primaryKeys: [['username'], 'age']
                });

            assert(userSchema instanceof Cassandra.Schema);
            assert(userSchema.fields);
            assert(userSchema.options);
            assert(userSchema.model);
        });
        it ('should fail at creating schema if there are no primaryKeys', () => {
            assert.throws(() => {
                new Cassandra.Schema({username: 'text', age: 'int'});
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
                primaryKeys: ['username']
                //primaryKeys: ['username', 'age'] //compound keys
                //primaryKeys: [['username'], 'age'] //composite keys
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

    it.skip ('should be able to attach static methods', () => {
    });

});
