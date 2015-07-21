var fs = require('fs');
var path = require('path');
var async = require('async');

var fileParser = require('./fileParser');

var url = 'mongodb://127.0.0.1:/node6';

var insertOp = require('./bulkInsert').init(url);

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

console.time("bulk insert");
console.time("UpTime");

var MAX_SIZE = 20000000;
var FILE_BATCH_SIZE = 100;
var SLICE_SIZE = 1000;
var filepath = path.resolve(__dirname, "../TES");

async.waterfall([
	function(callback){
		goThrough(filepath, function(err, paths){
            if(err) callback(err);
			console.log("Number of files : " + paths.length);
			callback(null, paths);
		});
	},
	function(paths, callback){
		var fileSlice = [];
		var partsCount = Math.ceil(paths.length / FILE_BATCH_SIZE);
		for(var i = 0; i < partsCount; i++){
			if(i != partsCount -1) {
				fileSlice.push(paths.slice(i * FILE_BATCH_SIZE, (i + 1) * FILE_BATCH_SIZE));
			} else {
				fileSlice.push(paths.slice(i * FILE_BATCH_SIZE, paths.length));
			}
		}

		var processCount = 0;
        var totalInserted = 0;
		async.eachSeries(fileSlice, function(slice, cn){
			var Recorder = new Object();
			Recorder.records = [];
			async.each(slice,
				function(file, cb){
					fileParser.parseFile(file, Recorder, cb);
			}, 	function(err){
				if(err) cn(err);
				insertOp.manyBulkInsert(Recorder.records, function(err, insertedCount) {
				    if(err) cn(err);
                    totalInserted += insertedCount;
			        console.log("Progress : " +  ++processCount / partsCount * 100 + " %...");
                    console.log("Total inserted Records : " + totalInserted);
                    console.timeEnd("UpTime");
                    if(totalInserted > MAX_SIZE){
                    	callback(null, "Insert Success. ");
                    }
                    cn();
				});
			});
		}, function(err){
			if(err) callback(err);
			callback(null, "Insert Success.");
		});
	}
],	function(err, output){
	if(err) {
		console.log(err);
		return;
	}
	console.log(output);
	console.timeEnd("bulk insert");
});
