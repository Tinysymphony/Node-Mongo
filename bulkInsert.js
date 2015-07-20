var async = require('async');
var client = require('mongodb').MongoClient;

var mongoose = require('mongoose'),
	Schema = mongoose.Schema,
	ObjectId = Schema.ObjectId;

var SLICE_SIZE = 1000;

var Insertion = new Object();

Insertion.init = function(url){
	this.url = url;
	mongoose.connect(url);
	this.taxiSchema = new Schema({
		id: ObjectId,
		l: [Number],
		t: Date,
		i: Boolean,
		v: Number,
		d: Number,
		n: String
	}, {"strict": false});
	this.TaxiRecord = mongoose.model("TaxiRecord", this.taxiSchema, "taxilog");
	return this;
}

Insertion.insertEveryThousand = function(docs, callback){

	var parts = Math.ceil(docs.length / SLICE_SIZE);
	var docsArray = thousandSlice(docs, parts);

	var pending = docsArray.length;
	var save = pending;
	if(!pending) return callback("docs is empty!");

	console.time("InsertTime / recursiveInsert");
	console.time("single insert");

	client.connect(this.url, function(err, db){
		if(err){
			console.log("cannot connect to database");
			callback(err);
		}
		console.time("InsertTime / manyBulkInsert");
		var col = db.collection("taxi");
		docsArray.forEach(function(docs){
			col.insertMany(docs, function(err){
				if(err) callback(err);
				console.timeEnd("single insert");
				if(!--pending) {
					db.close();
					console.timeEnd("InsertTime / recursiveInsert");
					callback(null, "Insert Success!");
				}
				console.log((save - pending) / parts * 100 + " % Finished");

			});
		});
	});
}

Insertion.manyBulkInsert = function(docs, callback){
	client.connect(this.url, function(err, db){
		if(err){
			console.log("cannot connect to database");
			callback(err);
		}
		console.time("InsertTime / manyBulkInsert");

		var col = db.collection("taxi");

		//batch insert
		var parts = Math.ceil(docs.length / SLICE_SIZE);
		var docsArray = thousandSlice(docs, parts);

		console.log("Sliced to " + parts + " parts");

		console.timeEnd("Before Insert");

		console.time("single insert");

		// var testArray = [
		// docsArray[3], docsArray[6], 
		// docsArray[7], docsArray[1],
		// docsArray[10],docsArray[11],
		// docsArray[40],docsArray[50]
		// ];

		// //single thousand insert test
		// col.insertMany(docsArray[5], function(err){
		// 	if(err) callback(err);
		// 	console.timeEnd("single insert");
		// 	console.timeEnd("InsertTime / manyBulkInsert");
		// 	callback(null, "Insert Success!");
		// });

		var insertCount = 0;
		async.each(docsArray,
			function(docPart, cb){
				col.insertMany(docPart, function(err){
					if(err) cb(err);
					console.timeEnd("single insert");
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

Insertion.bulkInsert = function(docs, callback){

	console.timeEnd("Before Insert");
	console.time("InsertTime / bulkInsert");

	client.connect(this.url, function(err, db){
		if(err){
			console.log("cannot connect to database");
			callback(err);
		}
		var col = db.collection("taxi");

		//batch insert
		// var batch = col.initializeOrderedBulkOp();
		var batch = col.initializeUnorderedBulkOp();

		var ddd = docs.slice(100, 1099);


		// //a thousand insert test
		// for(var index in ddd){
		// 	var doc = ddd[index];
		// 	batch.insert(doc);
		// }

		for(var index in docs){
			var doc = docs[index];
			batch.insert(doc);
		}

		batch.execute(function(err){
			if(err){
				console.log(err);
			}
			console.timeEnd("InsertTime / bulkInsert");
			callback(null, "Insert Success!");
			db.close();
		});
	});
}

Insertion.singleLoopInsert = function(docs, callback) {
	console.time("InsertTime / singleLoopInsert");

	client.connect(this.url, function(err, db){
		if(err){
			console.log("cannot connect to database");
			callback(err);
		}
		var col = db.collection("taxi");

		//a thousand insert test
		var ddd = docs.slice(0, 100000);
		async.each(ddd,
			function(doc, cb){
				col.insert(doc, function(err){
					if(err) cb(err);
					cb();
				});
			},
			function(err){
				if(err) callback(err);
				console.timeEnd("InsertTime / singleLoopInsert");
				callback(null, "Insert Success!");	
				db.close();
			}
		);
	});
}

Insertion.mongooseBulkInsert = function(docs, callback){
	console.time("InsertTime / mongooseBulkInsert");

	var parts = Math.ceil(docs.length / SLICE_SIZE);
	var docsArray = thousandSlice(docs, parts);
	
	// //a thousand insert test
	// this.TaxiRecord.collection.insert(docsArray[6], function(err){
	// 	if(err) callback(err);
	// 	mongoose.disconnect();
	// 	console.timeEnd("InsertTime / mongooseBulkInsert");
	// 	callback(null, "Insert Success");
	// });

	var insertCount = 0;
	async.each(docsArray,
		function(docPart, cb){
			this.TaxiRecord.collection.insert(docPart, function(err){
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

module.exports = Insertion;