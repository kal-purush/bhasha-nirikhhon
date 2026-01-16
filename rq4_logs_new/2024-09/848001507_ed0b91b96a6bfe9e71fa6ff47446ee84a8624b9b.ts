import Itineary from "../Activities/Itineary.js";
import Hotel from "../Hotel/Hotel.js";
import Person from "../Hotel/Person.js";
import Room from "../Hotel/Room.js";
import Trip from "../Trip/Trip.js";

export interface TripMap {
  [key: string]: Trip;
}

export interface ItinearyMap {
  [key: string]: Itineary
}

export interface HotelMap {
  [key: string]: Hotel;
}

export interface RoomMap {
  [key: string]: Room;
}

export interface TravellerMap {
  [key: string]: Person;
}