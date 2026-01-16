// Definisci lo schema e il modello per la collezione "Utenti"
const mongoose = require('mongoose');
const userSchema = require('./User');

const userSchema = new mongoose.Schema({
    descrizioneNotifica: {
        type: String
    },
    dataNotifica: {
        type: Date
    },
    operatore:{
        type: userSchema
    }
}, { collection: "notifiche" });

module.exports = userSchema;