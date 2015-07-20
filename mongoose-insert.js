
//created by Tiny 2015/7/20

var fs = require('fs');
var path = require('path');
var async = require('async');

var mongoose = require('mongoose'),
	Schema = mongoose.Schema,
	ObjectId = Schema.ObjectId;

var RECORD_SIZE = 39;
var SLICE_SIZE = 1000;

console.time("Before Insert");

var url = 'mongodb://127.0.0.1:27017/mongoose';

mongoose.connect(url);



