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

module.exports = class extends Service {

	constructor(context) {
		super(context);
	}

	async start() {

		this.getContext().set('StationCrawler', {
			getDataFromServer: () => {

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
			},
			parseData: (page) => {

				debug('Parsing web page')
				let $ = cheerio.load(page, {
					normalizeWhitespace: true,
					recognizeSelfClosing: true
				});

				let stations = $('table tbody tr').toArray();

				let unavaliable = false;

				return stations
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
			}
		});
	}

	async stop() {

		this.getContext().remove('StationCrawler');
	}
}
