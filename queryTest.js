var client = require('mongodb').MongoClient;
var url = 'mongodb://127.0.0.1:27017/node5';
console.time("Find Time");
client.connect(url, function (err, db) {
	var col = db.collection('taxi');
	col.find({v: 43, i:true}).toArray(function(err, docs){
		if(err){
			console.log(err);
			return;
		}
		console.log(docs.length);
		console.log(docs.pop());
		db.close();
		console.timeEnd("Find Time");
	});
});