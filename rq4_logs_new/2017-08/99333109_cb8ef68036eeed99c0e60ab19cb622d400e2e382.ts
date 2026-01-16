import { Gravector } from './gravector';

export class Jump {
  public gravector: Gravector[];
  public range: number;
  public efficiency: number;

  constructor(gravector: Gravector[], range: number, efficiency: number) {
    this.gravector = gravector;
    this.range = range;
    this.efficiency = efficiency;
  }
}