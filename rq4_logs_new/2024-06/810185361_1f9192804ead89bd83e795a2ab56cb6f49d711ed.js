const mongoose = require("mongoose")
const Schema = mongoose.Schema
require("dotenv").config()
const DataBase_URL = process.env.URL
console.log(DataBase_URL)

async function ensureInitialPartieDocument() {
    const initialDocument = await Partie.findOne({});
    if (!initialDocument) {
        await new Partie({}).save();
        console.log("Initial Partie document created.");
    }
}

main()
    .then(() => {
        ensureInitialPartieDocument();
        console.log("Successfully Connected to the dataBase")
    })
    .catch((err) => {
        console.log("Somthing went wrong while connecting to the database " + err)
    })

async function main() {
    mongoose.connect(DataBase_URL)
}


const Party = new Schema({
    BJP: { type: Number, default: 0 },
    INC: { type: Number, default: 0 },
    AAP: { type: Number, default: 0 }
})

const Users = new Schema({
    username: String,
    aadharNumber: String,
    voted: {
        type: Boolean,
        default: false
    },
    MyvoteTo: String
})

const Owner = new Schema({
    username: String,
    // password:Number,
    ElectionNumber: String,
    // ElectionNumber is same as aadhar number
    Isowner: Boolean
})

const Partie = mongoose.model("Partie", Party)
const Voter = mongoose.model("Voter", Users)
const PartyRegulator = mongoose.model("PartyRegulator", Owner)

module.exports = {
    Partie,
    Voter,
    PartyRegulator,
}