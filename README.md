# node-cassandra  [![Build Status][travis-badge]][travis-link] [![Coverage Status][coveralls-badge]][coveralls-link]


<!-- badge image references -->

[travis-badge]: https://travis-ci.org/vertebrae-org/node-cassandra.svg?branch=master
[travis-link]: https://travis-ci.org/vertebrae-org/node-cassandra

[coveralls-badge]: https://coveralls.io/repos/github/vertebrae-org/node-cassandra/badge.svg?branch=master
[coveralls-link]: https://coveralls.io/github/vertebrae-org/node-cassandra?branch=master

Cassandra ORM for NodeJS, based on the ***cassandra-driver*** module.

**Read the** [full API Documentation](http://vertebrae-org.github.io/node-cassandra).

**Examples:**
- [Getting Started](#user-content-getting-started)
    - [Connecting](#user-content-connecting-to-the-database)
    - [Your first Schema, Model, and Materialized View](#user-content-creating-your-first-schema-model-and-materialized-view)
- [Static Methods](#user-content-static-methods)
- [Working with List Types](#user-content-list-types)
    - [Inserting Lists](#user-content-inserting-lists)
    - [Updating Lists](#user-content-updating-lists)
    - [Updating Lists By Index](#user-content-updating-lists-by-index)
    - [Deleting Lists](#user-content-deleting-lists-by-column-or-index)
- [Working with Set Types](#user-content-set-types)
    - [Inserting Sets](#user-content-inserting-sets)
    - [Updating Sets](#user-content-updating-sets)
    - [Deleting Sets](#user-content-deleting-sets)
- [Working with Map Types](#user-content-map-types)
    - [Inserting Maps](#user-content-inserting-maps)
    - [Updating Maps](#user-content-updating-maps)
    - [Deleting Maps by Keys](#user-content-deleting-maps-by-keys)
    - [Deleting Maps](#user-content-deleting-maps)




<h4>Cassandra Known Versions Supported:</h4>
  - 3.0.4
  - 3.4

<h4>NodeJS Versions Supported:</h4>
  - 5.8.0
  - 5.7.1
  - 5.6.0
  - 5.5.0
  - 5.4.1
  - 5.3.0
  - 5.2.0
  - 5.1.1
  - 5.0.0



<h3>Getting Started</h3>

<h4>Connecting to the database</h4>

```javascript
var Cassandra = require('node-cassandra');
var config = {
    contactPoints: ['127.0.0.1:9042'],
    protocolOptions: {port: 9042},
    keyspace: {
        testkeyspace: {
            durableWrites: true,
            withReplication: {
                class: 'SimpleStrategy',
                replication_factor: 1
            }
        }
    }
};

var cassandra = Cassandra.connect(config);
cassandra.on('error', (err) => console.log(err));
cassandra.on('connect', (err) => console.log('connected'));
```


<h4>Creating your first Schema, Model and Materialized View</h4>

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

//create model
var TestModel = cassandra.model('testModel', schema, (err) => {
    test.save((err) => {
        if (err) {
            throw err;
        }
        console.log('test saved!');
        test.age = 29;
        test.save((err) => {...});
        //test.age updated
    });
});

var test = new TestModel({
    name: 'bar',
    username: 'foo',
    age: 30
});
```


<h3>Working with Collections and Indexes</h3>

<h4>List Types</h4>
```javascript
//create a new schema
var schema = new Cassandra.Schema({
    name: 'text',
    usernames: {
        type: {
            list: 'text'
        }
    }
}, {
    indexes: ['usernames'], //create an index on the usernames column
    primaryKeys: ['name']
});
```

<h5>Inserting lists</h5>
```javascript
//create model
var TestModel = cassandra.model('testModel', schema, (err) => {
    var test = new TestModel({
        name: 'bar',
        usernames: ['foo', 'bar']
    });
    test.save(() => {
        TestModel.findOne({
            usernames: {
                $contains: 'foo'
            }
        }, (err, row) => {
            console.log(row.usernames); //['foo', 'bar']
        });
    });
});
```

<h5>Updating lists</h5>
```javascript
//following above
var query = {
    name: 'bar',
    usernames: {
        $contains: 'foo'
    }
};
var updateObject = {
    usernames: {
        $filter: ['foo'], //remove items matching "foo"
        $append: ['baz'], //append item "baz"
        $prepend: ['fii'] //prepend item "fii"
    }
};
TestModel.update(query, updateObject, (err) => {
    if (err) {
        return console.log(err):
    }
    TestModel.findOne({usernames: {$contains: 'baz'}}, (err, row) => {
        console.log(row.usernames); //['fii', 'bar', 'baz']
    });
});
```

<h5>Updating lists by index</h5>
```javascript
//following above
var query = {
    name: 'bar',
    usernames: {
        $contains: 'bar'
    }
};
var updateObject = {
    usernames: {
        $set: {
            0: 'foo'
        }
    }
};
TestModel.update(query, updateObject, (err) => {
    if (err) {
        return console.log(err):
    }
    TestModel.findOne(query, (err, row) => {
        console.log(row.usernames); //['foo', 'bar', 'baz']
    });
});
```

<h5>Deleting lists by column or index</h5>
```javascript
//following above
var query = {
    name: 'bar',
    usernames: {
        $contains: 'fii'
    }
};
var deleteObject = {
    usernames: [0,2] //delete first and last index
};
TestModel.delete(query, deleteObject, (err) => {
    if (err) {
        return console.log(err):
    }
    TestModel.findOne({usernames: {$contains: 'bar'}}, (err, row) => {

        console.log(row.usernames); //['bar']

        TestModel.delete({
            usernames: {
                $contains: 'bar'
            }
        }, {
            usernames: 1 //delete usernames column
        }, (err) => {...});

    });
});
```


<h4>Set Types</h4>
```javascript
//create a new schema
var schema = new Cassandra.Schema({
    name: 'text',
    usernames: {
        type: {
            set: 'text'
        }
    }
}, {
    indexes: ['usernames'], //create an index on the usernames column
    primaryKeys: ['name']
});
```

<h5>Inserting sets</h5>
```javascript
//create model
var TestModel = cassandra.model('testModel', schema, (err) => {
    var test = new TestModel({
        name: 'bar',
        usernames: ['foo', 'bar']
    });
    test.save(() => {
        TestModel.findOne({
            usernames: {
                $contains: 'foo'
            }
        }, (err, row) => {
            console.log(row.usernames); //['bar', 'foo']
        });
    });
});
```

<h5>Updating sets</h5>
```javascript
//following above
var query = {
    name: 'bar',
    usernames: {
        $contains: 'bar'
    }
};
var updateObject = {
    usernames: {
        $add: ['aba', 'boa'],
        $filter: ['foo']
    }
};
TestModel.update(query, updateObject, (err) => {
    if (err) {
        return console.log(err):
    }
    TestModel.findOne(query, (err, row) => {
        console.log(row.usernames); //['aba', 'bar', 'boa']
    });
});
```

<h5>Update sets to null</h5>
```javascript
//following above
var query = {
    name: 'bar',
    usernames: {
        $contains: 'bar'
    }
};
var updateObject = {
    usernames: [] //or null
};
TestModel.update(query, updateObject, (err) => {
    if (err) {
        return console.log(err):
    }
    TestModel.findOne(query, (err, row) => {
        console.log(row.usernames); //null
    });
});
```

<h5>Deleting sets</h5>
```javascript
//not much you can do with deleting sets
var query = {
    name: 'bar',
    usernames: {
        $contains: 'fii'
    }
};
var deleteObject = {
    usernames: 1 //delete usernames column
};
TestModel.delete(query, deleteObject, (err) => {
    if (err) {
        return console.log(err):
    }
    TestModel.findOne({name: 'bar'}, (err, row) => {
        console.log(row.usernames); //null
    });
});
```


<h4>Map Types</h4>
```javascript
//create a new schema
var schema = new Cassandra.Schema({
    name: 'text',
    usernames: {
        type: {
            map: ['text', 'int']
        }
    }
}, {
    indexes: ['usernames'], //create an index on the usernames column keys
    primaryKeys: ['name']
});
```

<h5>Inserting maps</h5>
```javascript
//create model
var TestModel = cassandra.model('testModel', schema, (err) => {
    var test = new TestModel({
        name: 'bar',
        usernames: {
            $set: {
                foo: 2,
                bar: 4
            }
        }
    });
    test.save(() => {
        TestModel.findOne({
            usernames: {
                $containsKey: 'foo' //use containsKey for maps
            }
        }, (err, row) => {
            console.log(row.usernames.foo); // 2
        });
    });
});
```

<h5>Updating maps</h5>
```javascript
//following above
var query = {
    name: 'bar',
    usernames: {
        $containsKey: 'bar'
    }
};
var updateObject = {
    usernames: {
        foo: 5, //update "foo" to 5
        fii: 7 /set "fii" to 7
    }
};
TestModel.update(query, updateObject, (err) => {
    if (err) {
        return console.log(err):
    }
    TestModel.findOne(query, (err, row) => {
        console.log(row.usernames.foo); // 5
    });
});
```

<h5>Deleting maps by Keys</h5>
```javascript
//not much you can do with deleting maps
var query = {
    name: 'bar',
    usernames: {
        $containsKey: 'bar'
    }
};
var deleteObject = {
    usernames: ['foo']
};
TestModel.delete(query, deleteObject, (err) => {
    if (err) {
        return console.log(err):
    }
    TestModel.findOne(query, (err, row) => {
        console.log(row.usernames.foo); // undefined
    });
});
```

<h5>Deleting maps</h5>
```javascript
//not much you can do with deleting maps
var query = {
    name: 'bar',
    usernames: {
        $containsKey: 'bar'
    }
};
var deleteObject = {
    usernames: 1
};
TestModel.delete(query, deleteObject, (err) => {
    if (err) {
        return console.log(err):
    }
    TestModel.findOne(query, (err, row) => {
        console.log(row.usernames); // null
    });
});
```



Static Methods
--------------

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
    name: 'text'
}, {
    primaryKeys: ['username'],
    views: {
        byName: {
            primaryKeys: ['name']
        },
    }
});

//attach static method - fat arrow functions won't work here as we need the context to change
schema.statics.findByName = function (name, callback) {
    this.views.byName.findOne({name: name}, callback);  
};

//create model
var TestModel = cassandra.model('testModel', schema, (err) => {
    TestModel.findByName('foo', (err, row) => {
        console.log(row.name); // "foo"
    });
});
```

Stress Utility
--------------
node-cassandra includes an interactive command line utility.

* read - make n asynchronous read requests, with n iterations
* write - make multiple write requests, creates fake user data
* load - make n read requests per second (randomized timing), with n iterations
* count - count how many rows you have created with the write command
* clean - drop the keyspace and exit
* help - display the help text below

running `node ./stress --help`

```
NODE-CASSANDRA STRESS UTILITY

  SYNOPSIS
      node ./stress [OPTIONS]

  OPTIONS
      --host           cassandra host address (default=127.0.0.1)
      --port           cassandra listen port (default=9042)
      --keyspace       cassandra table keyspace (default=loadtest)
      --cluster        run tests in cluster mode
      --nodes          number of nodes to spawn in cluster mode (default=numCPUs)
      --help           show this help
```

* Setting "--cluster" will start the cli in cluster mode.  By default, one node will fork for each CPU core.  You can manually set the number of forks with "--nodes" option.
* In cluster mode read, write, and load tests will be performed in parallel on each node.  If you have 4 nodes running, then write 1000 rows, you will be writing 4000 rows to the database.
