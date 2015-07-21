var indexOp = require('./mongo-index');
var url = 'mongodb://127.0.0.1:27017/node5';
var targetIndex = {
	indexArray: [{t: 1}, {n: 1}, {loc: 1}],
	commentArray: [" on time", " on number", " on location"]
};
indexOp.createIndex(url, targetIndex);