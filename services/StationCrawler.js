const { Transform, Writable } = require('stream');
const config = require('config');
const BatchStream = require('batch-stream');
const schedule = require('node-schedule');
const moment = require('moment');
const debug = require('debug')('service:StationCrawler');
const request = require('request');
const cheerio = require('cheerio');
const iconv = require('iconv-lite');

const { Service } = require('engined');

const fieldTable = [
	'lat',
	'lng',
	'locationName',
	'stationId',
	'ELEV',
	'CITY',
	'address',
];

module.exports = class extends Service {

	constructor(context) {
		super(context);

		this.job = null;
	}

	getDataFromServer() {

		return new Promise((resolve) => {

			let buffer = [];

			request('http://e-service.cwb.gov.tw/wdps/obs/state.htm')
//				.pipe(require('fs').createWriteStream('test.htm'));
//			require('fs').createReadStream('test.htm')
				.pipe(iconv.decodeStream('CP950'))
				.on('data', (chunk) => {
					buffer.push(chunk.toString());
				})
				.on('end', () => {
					resolve(buffer.join(''));
				});
		});
	}

	async updateStationDatabase() {

		debug('Fetching from server')
		let page = await this.getDataFromServer();

		debug('Parsing web page')
		let $ = cheerio.load(page, {
			normalizeWhitespace: true,
			recognizeSelfClosing: true
		});

		let stations = $('table tbody tr').toArray();

		let unavaliable = false;
		let records = stations
			.map((line, index) => {

				if (index < 2)
					return null;

				if (unavaliable) {
					return null;
				}

				let $line = $(line);

				// Getting value of fields
				let [ stationId, locationName, ELEV, lng, lat, city, address ] = $line
					.find('td')
					.toArray()
					.map((value, index) => {
						if (index >= 7)
							return null;

						return $(value).text()
							.replace(/\n/g, '')
							.replace(/ /g, '')
					});

				if (!stationId ||
					stationId === '站號' ||
					stationId.substr(0, 2) === '一、' ||
					stationId.substr(0, 2) === '二、') {
					return null;
				}

				if (stationId.substr(0, 2) === '說明' ||
					stationId.substr(0, 2) === '三、') {
					unavaliable = true;
					return null;
				}

				return [
					Number.parseFloat(lat),
					Number.parseFloat(lng),
					locationName,
					stationId,
					Number.parseFloat(ELEV),
					city,
					address
				];
			})
			.filter(data => (data));

		try {
			debug('Save to database');

			// Save to database
			const dbAgent = this.getContext().get('MySQL').getAgent('default');

			let qstr = [
				'INSERT INTO `WeatherStations` (',
				fieldTable.map(fieldName => '`' + fieldName + '`').join(','),
				') VALUES ?'
			].join(' ');

			let [ ret ] = await dbAgent.query(qstr, [ records ]);
		} catch(e) {
			debug(e);
			throw e;
		}
	}

	async start() {

		await this.updateStationDatabase();

		// Initializing scheduler
		let rule = new schedule.RecurrenceRule();
		rule.hour = 0;

		this.job = schedule.scheduleJob(rule, () => {
		});
	}

	async stop() {
		this.getContext().remove('Rainfall');

		if (this.job = null)
			return;

		this.job.cancel();
	}
}
