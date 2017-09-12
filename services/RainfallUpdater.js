const { Transform, Writable } = require('stream');
const BatchStream = require('batch-stream');
const schedule = require('node-schedule');
const rainfall = require('weather-taiwan').Rainfall;
const config = require('config');
const moment = require('moment');
const debug = require('debug')('service:RainfallUpdater');

const { Service } = require('engined');

const fieldTable = {
	'lat': 'lat',
	'lon': 'lng',
	'locationName': 'locationName',
	'stationId': 'stationId',
	'obsTime': 'obsTime',
	'ELEV': 'ELEV',
	'RAIN': 'RAIN',
	'MIN_10': 'MIN_10',
	'HOUR_3': 'HOUR_3',
	'HOUR_6': 'HOUR_6',
	'HOUR_12': 'HOUR_12',
	'HOUR_24': 'HOUR_24',
	'NOW': 'NOW',
	'CITY': 'CITY',
	'CITY_SN': 'CITY_SN',
	'TOWN': 'TOWN',
	'TOWN_SN': 'TOWN_SN',
	'ATTRIBUTE': 'ATTRIBUTE'
};

module.exports = class extends Service {

	constructor(context) {
		super(context);

		this.job = null;
	}

	async start() {

		let rule = new schedule.RecurrenceRule();
		rule.minute = 30;

		this.job = schedule.scheduleJob(rule, () => {

			let self = this;

			debug('Connecting to server to fetch new weather information at ' + moment().format('YYYY-MM-DD HH:mm:ss'));

			// Preparing to fetch rainfall information from server
			let fetcher = rainfall.fetch(config.get('rainfallUpdater.key'));
			let parser = rainfall.parse();

			// Update database with 50 records at once
			let batch = new BatchStream({ size: 50 });

			fetcher
				.pipe(parser)
				.pipe(new Transform({
					objectMode: true,
					transform(data, encoding, callback) {

						// Convert string to Date object
						data.obsTime = moment.utc(data.obsTime).format('YYYY-MM-DD HH:mm:ss');

						// Getting field what we need
						let record = Object.keys(fieldTable).map((fieldName) => {
							return data[fieldName];
						});

						callback(null, record);
					}
				}))
				.pipe(batch)
				.pipe(new Writable({
					objectMode: true,
					write(data, encoding, callback) {

						(async () => {

							// Save to database
							const dbAgent = self.getContext().get('MySQL').getAgent('default');

							try {
								debug('Updating ' + data.length + ' records...');

								let qstr = [
									'INSERT INTO `WeatherRainfallData` (',
									Object.values(fieldTable).map(fieldName => '`' + fieldName + '`').join(','),
									') VALUES ?'
								].join(' ');

								let [ ret ] = await dbAgent.query(qstr, [ data ]);
							} catch(e) {
								debug(e);
								return callback(e);
							}

							callback();
						})()
					}
				}))
		});
	}

	async stop() {

		if (this.job = null)
			return;

		this.job.cancel();
	}
}
