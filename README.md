# node-cassandra  [![Build Status][travis-badge]][travis-link] [![Coverage Status][coveralls-badge]][coveralls-link]


<!-- badge image references -->

[travis-badge]: https://travis-ci.org/vertebrae-org/node-cassandra.svg?branch=master
[travis-link]: https://travis-ci.org/vertebrae-org/node-cassandra

[coveralls-badge]: https://coveralls.io/repos/github/vertebrae-org/node-cassandra/badge.svg?branch=master
[coveralls-link]: https://coveralls.io/github/vertebrae-org/node-cassandra?branch=master

Cassandra ORM for NodeJS, based on the ***cassandra-driver*** module.

Read the [full API Documentation](http://vertebrae-org.github.io/node-cassandra).

NodeJS Versions Supported:
  - 5.8.0
  - 5.7.1
  - 5.6.0
  - 5.5.0
  - 5.4.1
  - 5.3.0
  - 5.2.0
  - 5.1.1
  - 5.0.0


Examples
--------

- Getting Connected

```javacript
var Cassandra = require('node-cassandra');
var config = {
    contactPoints: ['127.0.0.1:9042'], 
    protocolOptions: {port: 9042},
    keyspace: {
        testkeyspace: {
            class: 'SimpleStrategy',
            replication_factor: 1
        },
        durableWrites: true
    }
};

var cassandra = Cassandra.connect(config);
cassandra.on('error', (err) => console.log(err));
cassandra.on('connect', (err) => console.log('connected'));
```


- Creating your first Schema, Model and Materialized View

```javascript
//create a new schema
var schema = new Cassandra.Schema({
    id: {
        type: 'uuid',
        default: Cassandra.uuid
    },
    username: {
        type: 'text',
        required: true
    },
    name: 'text',
    age: 'int'
}, {
    primaryKeys: ['username'],
    views: {
        byName: {
            primaryKeys: ['name'],
            orderBy: {
                name: 'asc'
            }
        },
    }
});

//attach static method
schema.statics.findFoo = function (callback) {
    this.views.byName.findOne({name: 'bar'}, callback);  
};

//create model
var TestModel = cassandra.model('testModel', schema); //optional, callback);
var test = new TestModel({
    name: 'bar',
    username: 'foo',
    age: 30
});

test.save((err) => {
    if (err) {
        throw err;
    }
    console.log('test saved!');
    test.age = 29;
    test.save();
    //test.age updated
});
```
