const Joi = require('joi');

module.exports = {
	WeatherStations: {
		columns: {
			id: Joi.number().integer().positive().required().default('AUTO_INCREMENT'),
			lat: Joi.number().required(),
			lng: Joi.number().required(),
			locationName: Joi.string().max(64).required(),
			stationId: Joi.string().max(32).required(),
			ELEV: Joi.number(),
			CITY: Joi.string().max(64).required(),
			address: Joi.string().max(128).required()
		},
		indexes: {
			PRIMARY: {
				type: 'primary',
				columns: [
					'id'
				]
			},
			stationId: {
				type: 'unique',
				columns: [
					'stationId'
				]
			}
		}
	},
	WeatherRainfallData: {
		columns: {
			id: Joi.number().integer().positive().required().default('AUTO_INCREMENT'),
			lat: Joi.number().required(),
			lng: Joi.number().required(),
			locationName: Joi.string().max(64).required(),
			stationId: Joi.string().max(32).required(),
			obsTime: Joi.date().required(),
			ELEV: Joi.number(),
			RAIN: Joi.number().integer(),
			MIN_10: Joi.number().integer(),
			HOUR_3: Joi.number().integer(),
			HOUR_6: Joi.number().integer(),
			HOUR_12: Joi.number().integer(),
			HOUR_24: Joi.number().integer(),
			NOW: Joi.number().integer(),
			CITY: Joi.string().max(64).required(),
			CITY_SN: Joi.string().max(32).required(),
			TOWN: Joi.string().max(64).required(),
			TOWN_SN: Joi.string().max(32).required(),
			ATTRIBUTE: Joi.string().max(128)
		},
		indexes: {
			PRIMARY: {
				type: 'primary',
				columns: [
					'id'
				]
			},
			stationIdAndTime: {
				type: 'unique',
				columns: [
					'stationId',
					'obsTime'
				]
			},
			time: {
				type: 'index',
				columns: [
					'obsTime'
				]
			}
		}
	}
};
