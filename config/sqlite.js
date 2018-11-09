'use strict';

// Load config file
const config = require('../config.json');

// Initialize MySQL connection via ORM
var Sequelize = require('sequelize');
var sequelize = new Sequelize(config.dbFile, {
    dialect: 'sqlite',
    storage: config.dbFile
});
module.exports.sequelize = sequelize;