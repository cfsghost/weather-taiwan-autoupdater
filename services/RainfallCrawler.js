const { Transform, Writable } = require('stream');
const BatchStream = require('batch-stream');
const schedule = require('node-schedule');
const rainfall = require('weather-taiwan').Rainfall;
const config = require('config');
const debug = require('debug')('service:RainfallCrawler');

const { Service } = require('engined');

module.exports = class extends Service {

	constructor(context) {
		super(context);

		this.job = null;
	}

	async start() {

		// Initializing scheduler
		let rule = new schedule.RecurrenceRule();
		rule.minute = 30;

		this.job = schedule.scheduleJob(rule, () => {

			// Getting rainfall agent
			const rainfallAgent = this.getContext().get('Rainfall');

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

						data.lng = data.lon;
						delete data.lon;

						callback(null, rainfallAgent.pack(data));
					}
				}))
				.pipe(batch)
				.pipe(new Writable({
					objectMode: true,
					write(data, encoding, callback) {

						(async () => {

							try {
								debug('Updating ' + data.length + ' records...');

								await rainfallAgent.update(data);
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
