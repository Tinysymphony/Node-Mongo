
//created by Tiny, 2015/7/16

var fs = require('fs');
var path = require('path');
var async = require('async');
var client = require('mongodb').MongoClient;

var indexOp = require('./mongo-index'); 

var url = 'mongodb://127.0.0.1:27017/node2';

var RECORD_SIZE = 39;
var SLICE_SIZE = 1000;

var goThrough = function(dir, callback) {
	var results = [];
	fs.readdir(dir, function(err, list){
		if(err) return callback(err);
		var pending = list.length;
		if(!pending) return callback(null, results);
		list.forEach(function(file){
			file = path.resolve(dir, file);
			fs.stat(file, function(err, stat){
				if(stat && stat.isDirectory()){
					goThrough(file, function(err, res){
						results = results.concat(res);
						if(!--pending) callback(null, results);
					});
				} else {
					results.push(file);
					if(!--pending) callback(null, results);
				}
			});
		});

	});
}

var recordParse = function(file, Recorder, callback){
	fs.readFile(file, function(err, buffer){
		if(err) throw err;
		var count = buffer.length / 39;
		for(var readCount = 0; readCount < count; readCount++){
			var record = new Object();
			var tmpBuffer = buffer.slice(readCount * RECORD_SIZE, (readCount + 1)* RECORD_SIZE - 1);
			var longtitude = tmpBuffer.readFloatBE(0);
			var latitude = tmpBuffer.readFloatBE(4);
		
			//to save the disk space, attributes are shortened.
			record.l = [longtitude, longtitude]; //location
			record.t = new Date(tmpBuffer.readInt32BE(8)); //time	
			record.i = (tmpBuffer.get(16) > 0);	//isPassengerIn
			record.v = tmpBuffer.get(18);	//speed
			record.d = tmpBuffer.get(20);	//direction
			record.n = file.split('/').pop();	//platenumber
			Recorder.records[Recorder.records.length] = record;
		}
		callback();
	});
}

var bulkInsert = function(docs, callback){
	client.connect(url, function(err, db){
		if(err){
			console.log("cannot connect to database");
			callback(err);
		}
		console.time("InsertTime");

		var col = db.collection("taxi");
		//batch insert
		var times = Math.ceil(docs.length / SLICE_SIZE);
		var docsArray = [];

		var functionArray = [];

		console.time("SliceTime");
		for(var i = 0 ; i < times; i ++){
			if(i != times - 1) {
				docsArray.push(docs.slice(i * SLICE_SIZE, (i + 1)* SLICE_SIZE - 1));
			} else {
				docsArray.push(docs.slice(i * SLICE_SIZE, docs.length - 1));
			}
		}
		// for(var i in docsArray){
		// 	functionArray.push(
		// 		function(cb){
		// 			col.insertMany(docsArray[i], function(err){
		// 				if(err) {
		// 					cb(err);
		// 					return;
		// 				}
		// 				cb(null);
		// 			});
		// 		}
		// 	);
		// }
		console.timeEnd("SliceTime");

		// console.log(functionArray);

		console.log(times);

		var insertCount = 0;

		// col.insert(docs, function(err){
		// 	if(err) callback(err);
		// 	db.close();
		// 	console.timeEnd("InsertTime");
		// 	callback(null, "Insert Success!");	
		// });

		async.each(docsArray,
			function(docPart, cb){
				col.insertMany(docPart, function(err){
					if(err) cb(err);
					console.log( ++insertCount / times * 100 + "% Finished.");
					cb();
				});
			},
			function(err){
				if(err) callback(err);
				db.close();
				console.timeEnd("InsertTime");
				callback(null, "Insert Success!");				
			}
		);


		// //single test
		// col.insertMany(docsArray[3], function(err){
		// 	if(err) callback(err);
		// 	console.log("all green.");
		// 	db.close();
		// 	console.timeEnd("InsertTime");
		// 	callback(null, "Insert Success!");	
		// });

		// col.insertMany(docs, function (err){
		// 	if(err){
		// 		console.log(err);
		// 	}
		// 	db.close();
		// });

		// async.parallel(functionArray, function(err, results){
		// 	if(err){
		// 		console.log(err);
		// 		db.close();
		// 		callback(err);
		// 	}
		// 	db.close();
		// 	console.timeEnd("InsertTime");
		// 	callback(null, "Insert Success!");	
		// });


		// db.close();
		// console.timeEnd("InsertTime");
		// callback(null, "Insert Success!");	


		//batch insert
		// var batch = col.initializeOrderedBulkOp({
		// 	useLegacyOps: true
		// });

		// var batch = col.initializeUnorderedBulkOp({
		// 	useLegacyOps: true
		// });

		// for(var index in docs){
		// 	var doc = docs[index];
		// 	batch.insert(doc);
		// }

		// batch.execute(function(err){
		// 	if(err){
		// 		console.log(err);
		// 	}
		// 	db.close();
		// 	console.timeEnd("InsertTime");
		// 	callback(null, "Insert Success!");
		// });


		// async.each(docs,
		// 	function(doc, cb){
		// 		col.insert(doc, function(err){
		// 			if(err) cb(err);
		// 			cb();
		// 		});
		// 	},
		// 	function(err){
		// 		if(err) callback(err);
		// 		db.close();
		// 		console.timeEnd("InsertTime");
		// 		callback(null, "Insert Success!");	
		// 	}
		// );

	});
}

var filepath = path.resolve(__dirname, "../TES");


async.waterfall([
	function(callback) {
		goThrough(filepath, function(err, paths){
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
				recordParse(file, Recorder, cb);
			},
			function(err){
				if(err) console.log(err);
				console.log("Number of records : " + Recorder.records.length);
				callback(null, Recorder);
			}
		);
	},
	function(Recorder, callback) {
		bulkInsert(Recorder.records, callback);
	}
],	function(err, output){
	if(err){
		console.log(err);
		return;
	}
	console.log(output);

	var targetIndex = {
		indexArray: [{t: 1}, {n: 1}, {loc: 1}],
		commentArray: [" on time", " on number", " on location"]
	};

	indexOp.createIndex(url, targetIndex);

});

