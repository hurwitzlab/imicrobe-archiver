'use strict';

// Load config file
var config = require('../config.json');

// Initialize MySQL connection via ORM
var Sequelize = require('sequelize');
var sequelize = new Sequelize(config.dbFile, {
    dialect: 'sqlite',
    storage: config.dbFile
});
module.exports.sequelize = sequelize;