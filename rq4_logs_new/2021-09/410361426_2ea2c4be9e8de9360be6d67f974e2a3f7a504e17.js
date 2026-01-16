var db = require('../connexion');

var Alert = {
    getAlerts: function(callback){
        return db.query('SELECT * from alert', callback);
    },
    createAlerts: function(Alert, callback){
        return db.query('Insert into alert(numero_du_conducteur, numero_du_membre, numero_du_trajet, message_alert) values(?,?,?,?)', 
        [Alert.numero_conducteur, Alert.numero_passager, Alert.numero_trajet, Alert.message_alert], callback);
    },
    deleteAlerts: function(Alert, callback){
        return db.query('DELETE from alert WHERE id_alert = ?', [Alert.id_alert], callback);
    }

}

module.exports = Alert;