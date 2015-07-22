var indexOp = require('./mongo-index');
var url = 'mongodb://127.0.0.1:27017/node5';
var targetIndex = {
	indexArray: [{t: 1}, {n: 1}, {l: 1}, {v: 1}, {d: 1}],
	commentArray: [" on time", " on number", " on location", " on speed", " on direction"]
};
indexOp.createIndex(url, targetIndex);