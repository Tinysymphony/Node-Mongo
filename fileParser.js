var fs = require('fs');
var path = require('path');

var RECORD_SIZE = 39;

var Parser = new Object();

Parser.parseFile = function(file, Recorder, callback) {
	fs.readFile(file, function(err, buffer){
		if(err) throw err;
		var count = Math.floor(buffer.length / 39);
		for(var readCount = 0; readCount < count; readCount++){
			var record = new Object();
			var tmpBuffer = buffer.slice(readCount * RECORD_SIZE, (readCount + 1) * RECORD_SIZE);
			var longtitude = tmpBuffer.readFloatBE(0);
			var latitude = tmpBuffer.readFloatBE(4);
            //to save the disk space, attributes are shortened.
			record.l = [longtitude, latitude]; //location
			record.t = new Date(tmpBuffer.readUIntBE(8,8));  //time
			record.i = (tmpBuffer.get(16) > 0);	//isPassengerIn
			record.v = tmpBuffer.get(18);	//speed
			record.d = tmpBuffer.get(20);	//direction
			record.n = (file.split('/').pop().split('.'))[0];	//platenumber
			Recorder.records[Recorder.records.length] = record;
			// console.log(record);
		}
		console.log(file + " : record parse finished...");
		callback();
	});
}

module.exports = Parser;
