var cassandra = require('../');
console.log(cassandra);
console.log(Object.keys(cassandra));
var db = cassandra.connect({
    contactPoints: ['127.0.0.1:9042'], 
    protocolOptions: {port: 9042}
});

db.on('error', (err) => {
    console.log(err);
});

db.on('connect', (res) => {
    console.log(res);
});

var type = cassandra.types;
var UserSchema = new cassandra.Schema({
    username: 'text',
    age: 'int'
}, {
    primaryKeys: ['username']
});

var UserModel = db.model('user', UserSchema);
console.log(UserModel);
