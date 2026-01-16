// src/Modules/GoTransit/TrainTrip.ts

import { Type } from 'class-transformer';
import { IsBoolean, IsDate, IsNumber, IsString } from 'class-validator';

export class GoTransitTrainTrip {
  @Type(() => Number)
  @IsNumber()
  public Cars: number;

  @Type(() => Number)
  @IsNumber()
  public TripNumber: number;

  @Type(() => String)
  public StartTime: string;

  @Type(() => String)
  public EndTime: string;

  @Type(() => String)
  @IsString()
  public LineCode: string;

  @Type(() => String)
  @IsString()
  public RouteNumber: string;

  @Type(() => String)
  @IsString()
  // TODO MOVE TO ENUM
  public VariantDir: string;

  @Type(() => String)
  @IsString()
  public Display: string;

  @Type(() => Number)
  @IsNumber()
  public Latitude: number;

  @Type(() => Number)
  @IsNumber()
  public Longitude: number;

  @Type(() => Boolean)
  @IsBoolean()
  public IsInMotion: boolean;

  @Type(() => Number)
  @IsNumber()
  public DelaySeconds: number;

  @Type(() => Number)
  @IsNumber()
  public Course: number;

  /**
   * TODO smart hook this up to an actual generic
   * station type shared across tranist agencys in the main coedebase
   */
  @Type(() => String)
  @IsString()
  public FirstStopCode: string;

  @Type(() => String)
  @IsString()
  public LastStopCode: string;

  @Type(() => String)
  @IsString()
  public PrevStopCode: string;

  @Type(() => String)
  @IsString()
  public AtStationCode?: string;

  @Type(() => Date)
  @IsDate()
  public ModifiedDate: Date;
}