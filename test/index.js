var cassandra = require('../');

describe('Cassandra', function (done) {
    var db;
    var TestSchema;

    before((done) => {
        db = cassandra.connect({
            contactPoints: ['127.0.0.1:9042'], 
            protocolOptions: {port: 9042},
            keyspace: {
                testKeyspace: {
                    with_replication: {
                        class: 'SimpleStrategy',
                        replication_factor: 1
                    }
                }
            }
        });
        db.on('error', done);
        db.on('connect', done);
    });


    it ('should create a Schema object', () => {
        //var type = cassandra.types;
        var UserSchema = new cassandra.Schema({
                username: 'text',
                age: 'int'
            }, {
                primaryKeys: ['username'],
                compoundKeys: ['username', 'age']
            });

        TestSchema = db.model('users', UserSchema);
        console.log(TestSchema);
    });

    it.skip ('should be able to create keyspaces if they don\'t exist', () => {
           
    });

    it.skip ('should be able to attach static methods', () => {
    });

});
