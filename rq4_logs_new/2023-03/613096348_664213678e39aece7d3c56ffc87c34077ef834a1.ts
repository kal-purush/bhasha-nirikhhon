// src/Modules/GoTransit/Trips.ts
import { IsDate, IsNumber, IsString } from 'class-validator';
import { Type } from 'class-transformer';
import { GoTransitStop } from './Stop';

export class GoTransitTrip {
  @Type(() => String)
  @IsString()
  public Info: string;

  @Type(() => Number)
  @IsNumber()
  public TripNumber: number;

  @Type(() => String)
  @IsString()
  public Platform: string;

  @Type(() => String)
  @IsString()
  public Service: string;

  @Type(() => String)
  @IsString()
  public ServiceType: string;

  @Type(() => Date)
  @IsDate()
  public Time: Date;

  @Type(() => GoTransitStop)
  public Stops: GoTransitStop[];
}