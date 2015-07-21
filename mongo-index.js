
var async = require('async');
var client = require('mongodb').MongoClient;

var Index = new Object();

Index.createIndex = function (url, indexObject) {

	if(!indexObject || ! indexObject instanceof Object){
		console.log("CREATE_INDEX: Please fill the second paragram with an Object {[indices], [comments]}");
		return;
	}

	var indexArray = indexObject.indexArray;
	var commentArray = indexObject.commentArray;

	console.log("Start creating indices...");

	client.connect(url, function(err, db){
		if(err){
			console.log("cannot connect to database");
			callback(err);
		}

		var col = db.collection("taxi");
		var count = 0;
		async.eachSeries(indexArray,
			function(item, callback){
				console.time("Index" + commentArray[count]);
				col.ensureIndex(item, function(err){
					if(err) throw(err);
					console.timeEnd("Index" + commentArray[count++]);
					callback();
				});
			}, function(err){
				if(err){
					console.log(err);
					return;
				}
				db.close();
				console.log("index has created.");
			}
		);
	});
}

module.exports = Index;