// src/Modules/GoTransit/Service/NextService.ts
import { Type } from 'class-transformer';
import { Service } from 'typedi';

enum DepartureStatus {
  ESTIMATED = 'E',
  CANCELLED = 'C',
  ACTUAL = 'A',
}

@Service()
export class GoTransitNextService {
  @Type(() => String)
  public StopCode: string;

  @Type(() => String)
  public LineCode: string;

  @Type(() => String)
  public LineName: string;

  @Type(() => String)
  public ServiceType: string;

  @Type(() => String)
  public DirectionCode: string;

  @Type(() => String)
  public DirectionName: string;

  @Type(() => Date)
  public ScheduledDepartureTime: Date;

  @Type(() => Date)
  public ComputedDepartureTime: Date;

  @Type(() => String)
  public DepartureStatus: DepartureStatus;

  @Type(() => Number)
  public ScheduledPlatform: number;

  @Type(() => Number)
  public ActualPlatform?: number;

  @Type(() => Number)
  public TripOrder: number;

  @Type(() => String)
  /**
   * TODO check if all number strings
   */
  public TripNumber: string;

  @Type(() => Date)
  public UpdateTime: Date;

  @Type(() => String)
  public Status: string;

  @Type(() => Number)
  public Latitude: number;

  @Type(() => Number)
  public Longitude: number;
}