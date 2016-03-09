const assert = require('assert');
const Cassandra = require('../');

describe('Cassandra', function (done) {
    before((done) => {
        const cassandra = Cassandra.connect({
            contactPoints: ['127.0.0.1:9042'], 
            protocolOptions: {port: 9042},
            keyspaces: {
                testkeyspace: {
                    'with replication': {
                        class: 'SimpleStrategy',
                        replication_factor: 1
                    },
                    durable_writes: true
                }
            }
        });
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

    it ('should be able to create keyspaces if they don\'t exist', () => {
           
    });

    it.skip ('should be able to attach static methods', () => {
    });

});
