var schedule = require('node-schedule');
var updater = require('./updater');

var rule = new schedule.RecurrenceRule();
rule.minute = 51;

schedule.scheduleJob(rule, function() {
	console.log('Starting to fetch new weather information at ' + Date().toString());
	updater();
});
