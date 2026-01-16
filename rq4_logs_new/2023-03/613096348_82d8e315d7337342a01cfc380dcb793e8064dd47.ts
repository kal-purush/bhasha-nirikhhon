// src/Modules/GoTransit/apiTypes.ts

interface APIMetadata {
  TimeStamp: string;

  ErrorCode: string;

  ErrorMessage: string;
}

export interface APIResponse {
  Metadata: APIMetadata;
}

interface Stop {
  Name: string;

  Code: null | unknown;
}

interface Trip {
  Info: string;

  TripNumber: string;

  Platform: string;

  Service: string;

  // TODO T/B?
  ServiceType: string;

  Time: string;

  Stops: Stop[];
}

export interface Departure {
  Trip: Trip[];
}

export interface AllDeparturesResponse extends APIResponse {
  AllDepartures: Departure;
}