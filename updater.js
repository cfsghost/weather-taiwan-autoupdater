var weather = require('weather-taiwan');
var mongodb = require('mongodb');
var config = require('./configs/config.json');
var Queue = require('./queue');

var queue = new Queue();

var MongoClient = mongodb.MongoClient;

module.exports = function() {
	MongoClient.connect(config.database.uri, {
		server: {
			sslValidate: false,
			auto_reconnect: true
		}
	},function(err, db) {
		if (err) {
			throw err;
		}

		var endOfLine = false;
		var tasks = 0;

		queue.on('ready', function(dataSets) {

			tasks++;

			console.log('Saving ' + dataSets.length + ' datasets ...');

			// Update to database
			var bulkData = dataSets.map(function(data) {
				return {
					updateOne: {
						filter: { stationId: data.stationId, obsTime: data.obsTime },
						update: data,
						upsert: true
					}
				}
			});

			db.collection(config.database.collection).bulkWrite(bulkData, function() {

				// Close connection if everthing is updated
				tasks--;
				if (tasks == 0 && endOfLine) {
					db.close();
				}
			});
		});

		// Fetching weather information
		var fetcher = weather.fetch(config.accessKey);

		var parser = weather.parse();

		parser.on('data', function(data) {
			data.obsTime = new Date(data.obsTime);
			queue.push(data);
		});

		parser.on('end', function() {
			queue.push(null);
			endOfLine = true;
		});

		fetcher.pipe(parser)

	});
};
