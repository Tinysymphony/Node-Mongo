
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

var recordParse = function(file, Recorder, callback){
	fs.readFile(file, function(err, buffer){
		if(err) throw err;
		var count = buffer.length / 39;
		for(var readCount = 0; readCount < count; readCount++){
			var record = new Object();
			var tmpBuffer = buffer.slice(readCount * RECORD_SIZE, (readCount + 1)* RECORD_SIZE - 1);
			var longtitude = tmpBuffer.readFloatBE(0);
			var latitude = tmpBuffer.readFloatBE(4);
			// if(longtitude < 118 || longtitude > 123 || latitude < 27 || latitude > 32)
				// continue;
			record.loc = [longtitude, longtitude];
			record.time = new Date(tmpBuffer.readInt32BE(8));	
			record.isPassengerIn = (tmpBuffer.get(16) > 0);
			record.speed = tmpBuffer.get(18);
			record.direction = tmpBuffer.get(20);
			record.num = file.split('/').pop();
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
		var batch = col.initializeUnorderedBulkOp({
			useLegacyOps: true
		});

		for(var index in docs){
			var doc = docs[index];
			batch.insert(doc);
		}
		batch.execute(function(err){
			if(err){
				console.log(err);
			}
			console.timeEnd("InsertTime");
			callback(null, "Insert Success!");
			db.close();
		});
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
});

