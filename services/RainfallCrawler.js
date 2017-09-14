const { Transform, Writable } = require('stream');
const BatchStream = require('batch-stream');
const schedule = require('node-schedule');
const rainfall = require('weather-taiwan').Rainfall;
const config = require('config');
const moment = require('moment');
const debug = require('debug')('service:RainfallCrawler');

const { Service } = require('engined');

module.exports = class extends Service {

	constructor(context) {
		super(context);
	}

	async start() {

		this.getContext().set('RainfallCrawler', {

			fetch: () => {

				return new Promise((resolve, reject) => {

					// Getting rainfall agent
					const rainfallAgent = this.getContext().get('Rainfall');

					debug('Connecting to server to fetch new weather information at ' + moment().format('YYYY-MM-DD HH:mm:ss'));

					// Preparing to fetch rainfall information from server
					let fetcher = rainfall.fetch(config.get('rainfallUpdater.key'));
					let parser = rainfall.parse();

					// Update database with 50 records at once
					let batch = new BatchStream({ size: 50 });

					// Stream for updating database
					let updateDatabase = new Writable({
						objectMode: true,
						write(data, encoding, callback) {

							(async () => {

								try {
									debug('Updating ' + data.length + ' records...');

									await rainfallAgent.update(data);
								} catch(e) {

									// Record exists already
									if (e.code === 'ER_DUP_ENTRY') {
										return callback();
									}

									debug(e);
									return callback(e);
								}

								callback();
							})()
						}
					});

					updateDatabase.on('finish', () => {
						resolve();
					});

					updateDatabase.on('error', (err) => {
						reject(err);
					});

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
						.pipe(updateDatabase);
				});
			}
		});
	}

	async stop() {

		this.getContext().remove('RainfallCrawler');
	}
}
