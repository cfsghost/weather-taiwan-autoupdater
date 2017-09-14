const config = require('config');
const moment = require('moment');
const debug = require('debug')('service:Database');

const { Service } = require('engined');

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
	}

	async stop() {
	}
}
