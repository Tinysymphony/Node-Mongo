var fs = require('fs');
var path = require('path');

var walk = function(dir, callback) {
	// console.log("walk through " + dir);
	var results = [];
	fs.readdir(dir, function(err, list){
		if(err) return callback(err);
		var pending = list.length;
		if(!pending) return callback(null, results);
		list.forEach(function(file){
			file = path.resolve(dir, file);
			fs.stat(file, function(err, stat){
				if(err) callback(err);
				if(stat && stat.isDirectory()){
					walk(file, function(err, res){
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

module.exports = walk;
