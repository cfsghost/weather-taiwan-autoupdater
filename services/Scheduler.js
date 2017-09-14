const config = require('config');
const schedule = require('node-schedule');
const debug = require('debug')('service:Scheduler');

const { Service } = require('engined');

module.exports = class extends Service {

	constructor(context) {
		super(context);

		this.stationCrawlerJob = null;
		this.rainfallCrawlerJob = null;
	}

	async initializeRainfallCrawler() {

		// Initializing scheduler
		let rule = new schedule.RecurrenceRule();
		rule.minute = 30;

		this.rainfallCrawlerJob = schedule.scheduleJob(rule, async () => {

			const rainfallCrawler = this.getContext().get('RainfallCrawler');

			try {
				await rainfallCrawler.fetch();
			} catch(e) {

				debug(e);
			}
		});
	}

	async initializeStationCrawler() {

		// Initializing scheduler
		let rule = new schedule.RecurrenceRule();
		rule.hour = 0;

		this.stationCrawlerJob = schedule.scheduleJob(rule, async () => {

			const stationCrawler = this.getContext().get('StationCrawler');
			const stationAgent = this.getContext().get('Station');

			try {

				// Getting web page which contains station list
				let page = await stationCrawler.getDataFromServer();

				// Parsing
				let records = stationCrawler.parseData(page);

				await stationAgent.update(records);

			} catch(e) {
				debug(e);
			}
		});
	}

	async start() {
		await this.initializeStationCrawler();
		await this.initializeRainfallCrawler();
	}

	async stop() {

		if (this.stationCrawlerJob) {
			this.stationCrawlerJob.cancel();
		}

		if (this.rainfallCrawlerJob) {
			this.rainfallCrawlerJob.cancel();
		}
	}
}
