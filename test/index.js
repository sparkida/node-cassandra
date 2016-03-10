const assert = require('assert');
const Cassandra = require('../');
const keyspaceName = 'testkeyspace';
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
    keyspaces: {}
};

config.keyspaces[keyspaceName] = keyspaceConfig;
var cassandra;

describe('Cassandra', function (done) {
    before((done) => {
        cassandra = Cassandra.connect(config);
        cassandra.on('error', done);
        cassandra.on('connect', done);
    });


    it ('should create a Schema object', () => {
        //var type = cassandra.types;
        var UserSchema = new Cassandra.Schema({
                username: 'text',
                age: 'int'
            }, {
                primaryKeys: [['username'], 'age']
            });

        assert(UserSchema instanceof Cassandra.Schema);
        assert(UserSchema.fields);
        assert(UserSchema.options);
        assert(UserSchema.model);
    });

    it ('should be able to create keyspaces if they don\'t exist', (done) => {
        cassandra.driver.metadata.refreshKeyspace('testkeyspace', (err, result) => {
            assert.equal(result.name, keyspaceName);
            done(err);
        });
    });

    it.skip ('should be able to attach static methods', () => {
    });

});
