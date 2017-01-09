var util = require('util');
var events = require('events');

var Queue = module.exports = function() {
	this.bufferSize = 50;
	this.queue = [];
};

util.inherits(Queue, events.EventEmitter);

Queue.prototype.push = function(data) {
	if (data)
		this.queue.push(data);

	if (data == null || this.queue.length >= this.bufferSize) {
		this.emit('ready', this.queue);
		this.queue = [];
	}
};
