'use strict';

// Load config file
const config = require('../config.json');

// Initialize MySQL connection via ORM
var Sequelize = require('sequelize');
var sequelize = new Sequelize(config.mysql.database, config.mysql.user, config.mysql.password, {
    host: config.mysql.host,
    dialect: 'mysql',
    define: {
        timestamps: false,
        freezeTableName: true,
        underscored: true
    },
    operatorsAliases: false
});
module.exports.sequelize = sequelize;