const config = require('config');
const moment = require('moment');
const debug = require('debug')('service:Station');

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
	}

	async start() {

		this.getContext().set('Station', {
			pack: (data) => {

				// Getting field what we need
				return fieldTable.map((fieldName) => {
					return record[fieldName];
				});
			},
			update: async (records) => {

				debug('Saving ...');

				// Save to database
				const dbAgent = this.getContext().get('MySQL').getAgent('default');

				let qstr = [
					'INSERT INTO `WeatherStations` (',
					fieldTable.map(fieldName => '`' + fieldName + '`').join(','),
					') VALUES ?'
				].join(' ');

				let [ ret ] = await dbAgent.query(qstr, [ records ]);

				debug('DONE')
			}
		});
	}

	async stop() {
		this.getContext().remove('Station');
	}
}
