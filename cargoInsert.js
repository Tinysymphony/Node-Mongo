var async = require('async');
var path = require('path');

var url = 'mongodb://127.0.0.1:27017/node6';
var fileParser = require('./fileParser');
var goThrough = require('./goThrough');
var insertOp = require('./bulkInsert').init(url);

var filepath = path.resolve(__dirname, "../TES");

console.time("UpTime");

var PAY_LOAD = 50;
var insertedCounter = 0;
var cargo = async.cargo(function (tasks, callback) {
    var Recorder = new Object();
    Recorder.records = [];
    async.each(tasks, function(task, cb){
        fileParser.parseFile(task.filepath, Recorder, cb);
    },function(err){
        if(err) callback(err);
        insertOp.manyBulkInsert(Recorder.records, function(err, insertedCount) {
            if(err) callback(err);
            insertedCounter += insertedCount;
            console.log("Total inserted Records : " + insertedCounter);
            callback();
        });
    });
}, PAY_LOAD);

async.waterfall([
    function(callback) {
        goThrough(filepath, function (err, paths) {
            if(err) callback(err);
            console.log(filepath);
            console.log(paths);
            console.log("Number of files : ", paths.length);
            callback(null, paths);
        });
    },
    function(paths, callback) {
        var counter = 0;
        async.each(paths, function (file, cb){
            cargo.push({filepath: file}, function(err){
                if(err) cb(err);
                console.log(++counter / paths.length * 100 + " % | " + file + " is inserted.");
                console.timeEnd("UpTime");
                cb();
            });
        }, function (err){
            if(err) callback(err);
            callback(null, "Insert Success");
        });
    }
], function (err, output) {
    if(err){
        console.log(err);
        return;
    }
    console.log(output);
    console.timeEnd("UpTime");
});
