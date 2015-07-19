
//created by Tiny, 2015/7/16

var fs = require('fs');
var path = require('path');
var async = require('async');
var client = require('mongodb').MongoClient;

var mongoose = require('mongoose'),
	Schema = mongoose.Schema,
	ObjectId = Schema.ObjectId;


var indexOp = require('./mongo-index'); 

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

var manyBulkInsert = function(docs, callback){
	client.connect(url, function(err, db){
		if(err){
			console.log("cannot connect to database");
			callback(err);
		}
		console.time("InsertTime / manyBulkInsert");

		var col = db.collection("taxi");

		//batch insert
		var parts = Math.ceil(docs.length / SLICE_SIZE);
		var docsArray = thousandSlice(docs, parts)

		console.log("Sliced to " + parts + " parts");

		var insertCount = 0;
		async.each(docsArray,
			function(docPart, cb){
				col.insertMany(docPart, function(err){
					if(err) cb(err);
					console.log( ++insertCount / parts * 100 + "% Finished.");
					cb();
				});
			},
			function(err){
				if(err) callback(err);
				db.close();
				console.timeEnd("InsertTime / manyBulkInsert");
				callback(null, "Insert Success!");				
			}
		);
	});
}

function bulkInsert(docs, callback){
	console.time("InsertTime / bulkInsert");

	client.connect(url, function(err, db){
		if(err){
			console.log("cannot connect to database");
			callback(err);
		}
		var col = db.collection("taxi");

		//batch insert
		var batch = col.initializeOrderedBulkOp();
		// var batch = col.initializeUnorderedBulkOp();

		for(var index in docs){
			var doc = docs[index];
			batch.insert(doc);
		}

		batch.execute(function(err){
			if(err){
				console.log(err);
			}
			db.close();
			console.timeEnd("InsertTime / bulkInsert");
			callback(null, "Insert Success!");
		});
	});
}

function singleLoopInsert(docs, callback) {
	console.time("InsertTime / singleLoopInsert");

	client.connect(url, function(err, db){
		if(err){
			console.log("cannot connect to database");
			callback(err);
		}
		var col = db.collection("taxi");
		async.each(docs,
			function(doc, cb){
				col.insert(doc, function(err){
					if(err) cb(err);
					cb();
				});
			},
			function(err){
				if(err) callback(err);
				db.close();
				console.timeEnd("InsertTime / singleLoopInsert");
				callback(null, "Insert Success!");	
			}
		);
	});
}

function mongooseBulkInsert(docs, callback){
	console.time("InsertTime / mongooseBulkInsert");

	var parts = Math.ceil(docs.length / SLICE_SIZE);
	var docsArray = thousandSlice(docs, parts);
	
	var insertCount = 0;
	async.each(docsArray,
		function(docPart, cb){
			TaxiRecord.collection.insert(docPart, function(err){
				if(err){
					cb(err);
				}
				console.log( ++insertCount / parts * 100 + "% Finished.");
				cb();
			});
		},
		function(err){
			if(err) callback(err);
			mongoose.disconnect();
			console.timeEnd("InsertTime / mongooseBulkInsert");
			callback(null, "Insert Success!");				
		}
	);
}

function thousandSlice(docs, parts){
	console.time("SliceTime");
	var docsArray = []; 
	for(var i = 0 ; i < parts; i ++){
		if(i != parts - 1) {
			docsArray.push(docs.slice(i * SLICE_SIZE, (i + 1)* SLICE_SIZE - 1));
		} else {
			docsArray.push(docs.slice(i * SLICE_SIZE, docs.length - 1));
		}
	}
	console.timeEnd("SliceTime");
	return docsArray;
}

/*-----------functions definition ends.-----------*/

var url = 'mongodb://127.0.0.1:27017/node3';

mongoose.connect(url);

var taxiSchema = new Schema({
		id: ObjectId,
		l: [Number],
		t: Date,
		i: Boolean,
		v: Number,
		d: Number,
		n: String
	}, {"strict": false}
);

var TaxiRecord = mongoose.model("TaxiRecord", taxiSchema);

var filepath = path.resolve(__dirname, "../TES");

console.time("InsertTime");

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
		var docs = Recorder.records;
		// manyBulkInsert(docs, callback);
		// mongooseBulkInsert(docs, callback);
		// bulkInsert(docs, callback);
		singleLoopInsert(docs, callback);
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

