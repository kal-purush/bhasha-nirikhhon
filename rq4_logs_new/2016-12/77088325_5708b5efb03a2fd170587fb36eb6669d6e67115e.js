var configValues = require('./config');

module.exports = {
    getDbConnectionString: function () {
      return ('mongodb://' + configValues.username + ':' +
            configValues.password +
            '@ds145168.mlab.com:45168/' + configValues.database);
            //mongodb://<dbuser>:<dbpassword>@ds145168.mlab.com:45168/flc_songs_db
    }
}