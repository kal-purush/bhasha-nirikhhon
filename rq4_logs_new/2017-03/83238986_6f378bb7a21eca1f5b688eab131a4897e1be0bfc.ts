const Sequelize = require( 'sequelize');

class DatabaseService {

    establishConnection() {
let sequelize = new Sequelize('neste', 'root', '', {
    host: 'localhost',
    dialect: 'mysql',

    pool: {
        max: 5,
        min: 0,
        idle: 10000
    }
});

    }
}

export default DatabaseService;