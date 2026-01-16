// src/Modules/GoTransit/Stop/StopDetails.ts

import { Type } from 'class-transformer';
import { IsString } from 'class-validator';
import { Service } from 'typedi';

@Service()
export class GoTransitPlaceStop {
  @Type(() => String)
  public Code: string;

  @Type(() => String)
  public Name: string;

  @Type(() => String)
  public NameFr: string;
}

@Service()
export class GoTransitPlaceStops {
  @Type(() => GoTransitPlaceStop)
  public Stop?: GoTransitPlaceStop[];
}

@Service()
export class GoTransitPlace {
  @Type(() => String)
  public Code: string;

  @Type(() => String)
  public Name: string;

  @Type(() => Number)
  public Longitude: number;

  @Type(() => Number)
  public Latitude: number;

  @Type(() => Number)
  public Radius: number;

  @Type(() => GoTransitPlaceStops)
  public Stops?: GoTransitPlaceStops;
}

export class GoTransitStopFacilities {
  @Type(() => String)
  @IsString()
  /**
   * TODO Figure these codes out to an ENUM
   */
  public Code: string;

  @Type(() => String)
  @IsString()
  public Description: string;

  @Type(() => String)
  @IsString()
  public DescriptionFr: string;
}

@Service()
export class GoTransitStopDetails {
  @Type(() => String)
  public ZoneCode: string;

  @Type(() => String)
  public StreetNumber: string;

  @Type(() => String)
  public Intersection: string;

  @Type(() => String)
  public City: string;

  @Type(() => String)
  public Code: string;

  @Type(() => String)
  public StopName: string;

  @Type(() => String)
  public StopNameFr: string;

  @Type(() => Boolean)
  public IsBus: boolean;

  @Type(() => Boolean)
  public IsTrain: boolean;

  @Type(() => Number)
  public Longitude: number;

  @Type(() => Number)
  public Latitude: number;

  @Type(() => String)
  public DrivingDirections?: string;

  @Type(() => String)
  public DrivingDirectionsFr?: string;

  public BoardingInfo?: string;

  public BoardingInfoFr?: string;

  @Type(() => GoTransitStopFacilities)
  public Facilities: GoTransitStopFacilities[];

  public Parkings?: any[];

  @Type(() => GoTransitPlace)
  public Place: GoTransitPlace;
}