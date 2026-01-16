import mongoose from "mongoose";
import { FlightData } from '../../order/FlightData';

const flightSchema = new mongoose.Schema({
  flightId: String,
  airline: String,
  flightNumber: String,
  departure: {
    airportCode: String,
    time: Date,
    terminal: String,
    gate: String,
  },
  arrival: {
    airportCode: String,
    time: Date,
    terminal: String,
    gate: String,
  },
  aircraft: {
    model: String,
    capacity: Number,
  },
});

const Flight = mongoose.model('Flight', flightSchema);

const MONGODB_URL = process.env.MONGO_URI;

mongoose.connect(MONGODB_URL, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
}).then(() => {
  console.log("Connected to MongoDD Atlas"),
  addFlights();
}).catch(err => console.error("Error Connecting MongoDB Atlas: ", err));

async function addFlights() {
  try {
    const flightSchedule = [
      {
        flightsSchedule: [
          {
            flightId: "MI001",
            airline: "MIAT",
            flightNumber: "OM501",
            departure: {
              airportCode: "UB",
              time: "2024-05-20T08:00:00",
              terminal: "1",
              gate: "5A",
            },
            arrival: {
              airportCode: "HND",
              time: "2024-05-20T16:00:00",
              terminal: "2",
              gate: "15C",
            },
            aircraft: {
              model: "Boeing 767",
              capacity: 225,
            },
          },
          {
            flightId: "MI002",
            airline: "MIAT",
            flightNumber: "OM303",
            departure: {
              airportCode: "UB",
              time: "2024-05-20T09:30:00",
              terminal: "1",
              gate: "6B",
            },
            arrival: {
              airportCode: "PEK",
              time: "2024-05-20T11:45:00",
              terminal: "3",
              gate: "27A",
            },
            aircraft: {
              model: "Boeing 737",
              capacity: 160,
            },
          },
          {
            flightId: "MI003",
            airline: "MIAT",
            flightNumber: "OM225",
            departure: {
              airportCode: "UB",
              time: "2024-05-20T14:00:00",
              terminal: "1",
              gate: "7C",
            },
            arrival: {
              airportCode: "ICN",
              time: "2024-05-20T18:00:00",
              terminal: "2",
              gate: "22D",
            },
            aircraft: {
              model: "Boeing 767",
              capacity: 225,
            },
          },
          {
            flightId: "MI004",
            airline: "MIAT",
            flightNumber: "OM707",
            departure: {
              airportCode: "UB",
              time: "2024-05-20T18:15:00",
              terminal: "1",
              gate: "8D",
            },
            arrival: {
              airportCode: "SVO",
              time: "2024-05-20T20:30:00",
              terminal: "F",
              gate: "5",
            },
            aircraft: {
              model: "Airbus A330",
              capacity: 250,
            },
          },
          {
            flightId: "MI005",
            airline: "MIAT",
            flightNumber: "OM601",
            departure: {
              airportCode: "UB",
              time: "2024-05-20T21:00:00",
              terminal: "1",
              gate: "9E",
            },
            arrival: {
              airportCode: "FRA",
              time: "2024-05-21T03:00:00",
              terminal: "1",
              gate: "14B",
            },
            aircraft: {
              model: "Airbus A330",
              capacity: 250,
            },
          },
          {
            flightId: "MI006",
            airline: "MIAT",
            flightNumber: "OM405",
            departure: {
              airportCode: "UB",
              time: "2024-05-21T07:00:00",
              terminal: "1",
              gate: "10F",
            },
            arrival: {
              airportCode: "DXB",
              time: "2024-05-21T10:00:00",
              terminal: "3",
              gate: "C1",
            },
            aircraft: {
              model: "Boeing 767",
              capacity: 225,
            },
          },
          {
            flightId: "MI007",
            airline: "MIAT",
            flightNumber: "OM807",
            departure: {
              airportCode: "UB",
              time: "2024-05-21T10:00:00",
              terminal: "1",
              gate: "11G",
            },
            arrival: {
              airportCode: "DEL",
              time: "2024-05-21T13:00:00",
              terminal: "3",
              gate: "18A",
            },
            aircraft: {
              model: "Boeing 737",
              capacity: 160,
            },
          },
          {
            flightId: "MI008",
            airline: "MIAT",
            flightNumber: "OM911",
            departure: {
              airportCode: "UB",
              time: "2024-05-21T12:30:00",
              terminal: "1",
              gate: "12H",
            },
            arrival: {
              airportCode: "JFK",
              time: "2024-05-21T20:30:00",
              terminal: "4",
              gate: "33Z",
            },
            aircraft: {
              model: "Airbus A330",
              capacity: 250,
            },
          },
          {
            flightId: "MI009",
            airline: "MIAT",
            flightNumber: "OM505",
            departure: {
              airportCode: "UB",
              time: "2024-05-21T15:00:00",
              terminal: "1",
              gate: "13I",
            },
            arrival: {
              airportCode: "HKG",
              time: "2024-05-21T19:00:00",
              terminal: "1",
              gate: "16K",
            },
            aircraft: {
              model: "Boeing 767",
              capacity: 225,
            },
          },
          {
            flightId: "MI010",
            airline: "MIAT",
            flightNumber: "OM605",
            departure: {
              airportCode: "UB",
              time: "2024-05-21T17:00:00",
              terminal: "1",
              gate: "14J",
            },
            arrival: {
              airportCode: "LHR",
              time: "2024-05-21T21:00:00",
              terminal: "5",
              gate: "40L",
            },
            aircraft: {
              model: "Airbus A330",
              capacity: 250,
            },
          },
        ],
      },
    ];

    for(const flight of flightSchedule) {
      await addFlight(flight);
    }
    console.log("All flights added Successfully");
  } catch (error) {
    console.error("Error adding flights: ", error);
  }
}

async function addFlight(FlightData: any) {
  try {
    const newFlight = new Flight(FlightData);
    await newFlight.save();
    console.log(`flight ${FlightData.flightId} added successfully`)
  } catch (error) {
    console.error(`Error adding flight ${FlightData.flightId}: `,error)
  }
}