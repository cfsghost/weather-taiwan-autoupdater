const config = require('config');
const moment = require('moment');
const debug = require('debug')('service:Rainfall');

const { Service } = require('engined');

const fieldTable = [
	'lat',
	'lng',
	'locationName',
	'stationId',
	'obsTime',
	'ELEV',
	'RAIN',
	'MIN_10',
	'HOUR_3',
	'HOUR_6',
	'HOUR_12',
	'HOUR_24',
	'NOW',
	'CITY',
	'ATTRIBUTE'
];

const fieldDefault = {
	'ELEV': -99999,
	'RAIN': 0,
	'MIN_10': 0,
	'HOUR_3': 0,
	'HOUR_6': 0,
	'HOUR_12': 0,
	'HOUR_24': 0,
	'NOW': 0,
	'ATTRIBUTE': ''
};

module.exports = class extends Service {

	constructor(context) {
		super(context);

		this.job = null;
	}

	async initializeDatabase() {

		const dbAgent = this.getContext().get('MySQL').getAgent('default');

		// Getting database models
		const models = require('../models/database');

		// Assert database
		await dbAgent.assertModels(models);
	}

	async start() {

		await this.initializeDatabase();

		this.getContext().set('Rainfall', {
			pack: (data) => {

				// Apply default value
				let record = Object.assign({}, fieldDefault, data);

				if (record.obsTime) {

					// Convert string to Date object
					record.obsTime = moment.utc(record.obsTime).format('YYYY-MM-DD HH:mm:ss');
				}

				// Getting field what we need
				return fieldTable.map((fieldName) => {
					return record[fieldName];
				});
			},
			update: async (records) => {

				// Save to database
				const dbAgent = this.getContext().get('MySQL').getAgent('default');

				let qstr = [
					'INSERT INTO `WeatherRainfallData` (',
					fieldTable.map(fieldName => '`' + fieldName + '`').join(','),
					') VALUES ?'
				].join(' ');

				let [ ret ] = await dbAgent.query(qstr, [ records ]);
			}
		});
	}

	async stop() {
		this.getContext().remove('Rainfall');
	}
}
