
//created by Tiny, 2015/7/16

var fs = require('fs');
var path = require('path');
var async = require('async');
var client = require('mongodb').MongoClient;
var url = 'mongodb://127.0.0.1:27017/node2';
var RECORD_SIZE = 39;

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

var recordParse = function(file, batch, callback){
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
			// Recorder.records[Recorder.records.length] = record;
			batch.insert(record);
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

		var col = db.collection("taxi4");
		//batch insert
		var times = Math.ceil(docs.length / 1000);
		var docsArray = [];

		console.time("slice");

		for(var i = 0 ; i < times; i ++){
			if(i != times - 1) {
				docsArray.push(docs.slice(i * 1000, (i + 1)* 1000 - 1));
			} else {
				docsArray.push(docs.slice(i * 1000, docs.length - 1));
			}
		}

		console.timeEnd("slice");

		console.log(times);

		var insertCount = 0;

		async.each(docsArray,
			function(docPart, cb){
				console.time("single");
				col.insertMany(docPart, function(err){
					if(err) cb(err);
					console.log( ++insertCount / times + "% Finished.");
					cb();
					console.timeEnd("single");
				});
			},
			function(err){
				if(err) callback(err);
				db.close();
				console.timeEnd("InsertTime");
				callback(null, "Insert Success!");				
			}
		);

		// col.insertMany(docs, function (err){
		// 	if(err){
		// 		console.log(err);
		// 	}
		// 	db.close();
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
		// 	console.timeEnd("InsertTime");
		// 	callback(null, "Insert Success!");
		// 	db.close();
		// });
	});
}

var filepath = path.resolve(__dirname, "../TES");


client.connect(url, function(err, db){
	if(err){
		console.log("cannot connect to database");
		callback(err);
	}
	console.time("InsertTime");

	var col = db.collection("taxi4");

	var batch = col.initializeOrderedBulkOp({
		useLegacyOps: true
	});

	async.waterfall([
		function(callback) {
			goThrough(filepath, function(err, paths){
				console.log("Number of files : " + paths.length);
				callback(null, paths)
			});
		},
		function(paths, callback) {
			// var records = [];
			async.each(paths,
				function(file, cb){
					recordParse(file, batch, cb);
				},
				function(err){
					if(err) console.log(err);
					console.log("Number of records : ");
					callback(null);
				}
			);
		},
		function(water, callback) {
			batch.execute(function(err){
				if(err) callback(err);
				db.close();
				console.timeEnd("InsertTime");
				callback(null, "Insert Success!");
			});
		}
	],	function(err, output){
		if(err){
			console.log(err);
			return;
		}
		console.log(output);
	});

});