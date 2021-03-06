
//created by Tiny, 2015/7/16
//This js code is supposed to test insert methods and index methods.
//slicedBulkInsert.js is currently the best resolution.

var path = require('path');
var async = require('async');

var url = 'mongodb://127.0.0.1:27017/node4';

var indexOp = require('./mongo-index');
var fileParser = require('./fileParser');
var insertOp = require('./bulkInsert').init(url);
var goThrough = require('./goThrough');

//start
console.time("Before Insert");

var filepath = path.resolve(__dirname, "../TES");

console.time("InsertTime");

async.waterfall([
	function(callback) {
		goThrough(filepath, function(err, paths){
			if(err) callback(err);
			console.log("Number of files : " + paths.length);
			callback(null, paths)
		});
	},
	function(paths, callback) {
		// var records = [];
		var Recorder = new Object();
		Recorder.records = [];
		async.each(paths,
			function(file, cb){
				fileParser.parseFile(file, Recorder, cb);
			},
			function(err){
				if(err) console.log(err);
				console.log("Number of records : " + Recorder.records.length);
				callback(null, Recorder);
			}
		);
	},
	function(Recorder, callback) {
		var docs = Recorder.records;
		insertOp.manyBulkInsert(docs, callback);
		// insertOp.insertEveryThousand(docs, callback);
		// insertOp.mongooseBulkInsert(docs, callback);
		// insertOp.bulkInsert(docs, callback);
		// insertOp.singleLoopInsert(docs, callback);
	}
],	function(err, output){
	if(err){
		console.log(err);
		return;
	}
	console.log(output);
	// return;
	var targetIndex = {
		indexArray: [{t: 1}, {n: 1}, {loc: 1}],
		commentArray: [" on time", " on number", " on location"]
	};
	indexOp.createIndex(url, targetIndex);
});
