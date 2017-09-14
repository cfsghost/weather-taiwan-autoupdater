const fs = require('fs');
const events = require('events');
const { Transform, Writable } = require('stream');
const commander = require('commander');
const saxStream = require('sax-stream');
const BatchStream = require('batch-stream');
const moment = require('moment');

const initializeServices = async () => {

	process.env.NODE_CONFIG_DIR = __dirname + '/../config';
	const config = require('config');
	const { Manager, Context } = require('engined');

	let ctx = new Context();

	try {

		// Loading services
		let services = require('../services');

		// We only pick up services what we need
		let selected = [
			'MySQL',
			'Database',
			'Rainfall',
			'Station',
			'StationCrawler',
			'RainfallCrawler',
		];

		let selectedServices = selected.reduce((result, serviceName) => {
			result[serviceName] = services[serviceName];
			return result;
		}, {});

		// Initializing engines
		let serviceManager = new Manager(ctx, { verbose: false });
		await serviceManager.loadServices(selectedServices);
		await serviceManager.startAll();

		// Force to stop instance
		process.on('SIGTERM', async () => {
			console.log('Stopping instance');
			await serviceManager.stopAll();
		});

		return serviceManager;
	} catch(e) {
		console.error(e);
	}
};

const state = new events.EventEmitter();

const processData = async (serviceManager, input) => {

	// Getting rainfall agent
	const rainfallAgent = serviceManager.getContext().get('Rainfall');

	const getStationInfo = async (stationId) => {
		const dbAgent = serviceManager.ctx.get('MySQL').getAgent('default');

		let qstr = [
			'SELECT lat, lng, locationName, stationId, ELEV, CITY FROM `WeatherStations`',
			'WHERE `stationId` = ? LIMIT 1'
		].join(' ');

		let [ ret ] = await dbAgent.query(qstr, [ stationId ]);

		if (!ret.length)
			return null;

		return ret[0];
	};

	const prepareTimeline = (data) => {

		// Getting data what we need
		return data.children.time
			.map(timeinfo => {
				return [
					moment(timeinfo.children.dataTime.value).utcOffset(8),
					Number.parseFloat(timeinfo.children.elementValue.children.value.value)
				];
			})
			.filter(([ time, value ]) => {
				return !isNaN(value);
			});
	};

	const prepareData = (stationInfo, time, value) => {

		return rainfallAgent.pack(Object.assign({}, stationInfo, {
			obsTime: time,
			NOW: value
		}));
	};

	const generateDayRecords = (stationInfo, time, value) => {

		let records = [];
		let record = prepareData(stationInfo, time, value);

		records.push(record);

		// Generate records for each hour
		for (let hour = 1; hour < 24; hour++) {
			
			let _time = moment(time).hours(hour);

			let newRecord = prepareData(stationInfo, _time, value);

			records.push(newRecord);
		}

		return records;
	}

	// Initializing stream
	const batchStream = new BatchStream({ size: 100 });
	const databaseStream = new Writable({
		objectMode: true,
		write(data, encoding, callback) {

			(async () => {

				try {
					await rainfallAgent.update(data);
				} catch(e) {

					// Record exists already
					if (e.code === 'ER_DUP_ENTRY') {
						return callback();
					}

					return callback(e);
				}

				callback();
			})();
		}
	});
	const dataProcess = new Transform({
		objectMode: true,
		transform(data, encoding, callback) {

			let stationId = data.children.stationId.value;

			(async () => {
				let stationInfo = await getStationInfo(stationId);

				if (stationInfo === null) {
					console.log('No such station:', stationId);
					callback();
					return;
				}

				console.log('Importing', stationInfo.locationName, 'data ...');

				let timeline = prepareTimeline(data.children.weatherElement);

				for (let index in timeline) {
					let [ time, value ] = timeline[index];

					generateDayRecords(stationInfo, time, value)
						.forEach(record => {
							// Push to next stream
							this.push(record);
						});
				}

				callback();
			})();
		}
	});

	await new Promise((resolve) => {

		databaseStream.on('finish', () => {
			console.log('done');
			resolve();
		});

		input
			.pipe(saxStream({
				strict: true,
				tag: 'location'
			}))
			.pipe(dataProcess)
			.pipe(batchStream)
			.pipe(databaseStream);
	});
};

const processFile = async (serviceManager, filename) => {

	let input = fs.createReadStream(__dirname + '/../data/' + filename);

	await processData(serviceManager, input);
};

state.on('import_all', async () => {

	let serviceManager = await initializeServices();

	console.log('Reading file');
	fs.readdir(__dirname + '/../data', async (err, files) => {

		for (let index in files) {
			let file = files[index];
			console.log('Loading', file, '...');
			await processFile(serviceManager, file);
		}

		process.exit();
	});
});

state.on('import_year', async () => {

	let serviceManager = await initializeServices();

	const config = require('config');
	const request = require('request');

	// Getting rainfall agent
	const rainfallAgent = serviceManager.getContext().get('Rainfall');

	console.log('Connecting to server to fetch data ...');

	// Update database with 50 records at once
	let batch = new BatchStream({ size: 50 });

	let input = request('http://opendata.cwb.gov.tw/opendataapi?dataid=C-B0025-001&authorizationkey=' + config.get('rainfallUpdater.key'))

	await processData(serviceManager, input);

	process.exit();
});

state.on('import_station', async () => {

	let serviceManager = await initializeServices();

	// Getting station agents
	const stationAgent = serviceManager.getContext().get('Station');
	const stationCrawler = serviceManager.getContext().get('StationCrawler');

	try {


		console.log('Connecting to server to fetch data ...');

		// Getting web page which contains station list
		let page = await stationCrawler.getDataFromServer();

		console.log('Parsing data ...');

		// Parsing
		let records = stationCrawler.parseData(page);

		console.log('Saving ...');

		// Save to database
		await stationAgent.update(records);

	} catch(e) {
		console.log(e);
	}

	process.exit();
});

state.on('fetch_now', async () => {

	let serviceManager = await initializeServices();

	// Getting station agents
	const rainfallCrawler = serviceManager.getContext().get('RainfallCrawler');

	try {

		console.log('Connecting to server to fetch data ...');

		await rainfallCrawler.fetch();

	} catch(e) {
		console.log(e);
	}

	process.exit();
});

commander.version('0.0.1');

commander
	.command('import_all')
	.description('import all existing data')
	.action(() => {
		state.emit('import_all');
	});

commander
	.command('import_year')
	.description('import for this year')
	.action(() => {
		state.emit('import_year');
	});

commander
	.command('import_station')
	.description('import station information')
	.action(() => {
		state.emit('import_station');
	});

commander
	.command('fetch_now')
	.description('fetch weather information now')
	.action(() => {
		state.emit('fetch_now');
	});

commander.parse(process.argv);

if (!process.argv.slice(2).length) {
	commander.help();
}
