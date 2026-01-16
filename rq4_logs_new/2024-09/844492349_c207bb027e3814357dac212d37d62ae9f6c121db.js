const mongoose = require('mongoose');

const Schema = mongoose.Schema;

const modelSchema = new Schema({
    "players": {
      "type": [
        "Mixed"
      ]
    },
    "map": {
      "name": {
        "type": "String"
      },
      "description": {
        "type": "String"
      },
      "author": {
        "type": "String"
      },
      "public": {
        "type": "Boolean"
      },
      "craeateDate": {
        "type": "Date"
      },
      "lastModifiedDate": {
        "type": "Date"
      },
      "rate": {
        "type": "Number"
      },
      "xSize": {
        "type": "Number"
      },
      "ySize": {
        "type": "Number"
      },
      "arrayTerrain": {
        "type": [
          "Mixed"
        ]
      }
    },
    "units": {
      "type": [
        "Mixed"
      ]
    },
    "currentPlayer": {
      "type": "Number"
    },
    "currentTurn": {
      "type": "Number"
    },
    "gameState": {
      "type": "Number"
    }
});

const model = mongoose.model('Game', modelSchema);

module.exports = model;