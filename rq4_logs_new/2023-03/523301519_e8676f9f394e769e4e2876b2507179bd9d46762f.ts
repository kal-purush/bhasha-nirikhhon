import IBag from "./IBag.model";
import ITravelClass from "./ITravelClass.model";
import IFlight from "./IFlight.model";
import IAirport from "./IAirport.model";
import IAircraft from "./IAircraft.model";

interface IFlightLegBag {
  bag: IBag;
  price: number;
  isActive: boolean;
}

interface IFlightLegTravelClass {
  travelClass: ITravelClass;
  price: number;
  isActive: boolean;
}

export default interface IFlightLeg {
  flightLegId: number;
  flightCode: string;
  flightId: number;
  originAirportId: number;
  destinationAirportId: number;
  departureDateAndTime: string;
  arrivalDateAndTime: string;
  aircraftId: number;
  isActive: boolean;

  flight?: IFlight;
  bags?: IFlightLegBag[];
  travelClasses?: IFlightLegTravelClass[];
  originAirport?: IAirport;
  destinationAirport?: IAirport;
  aircraft?: IAircraft;
}