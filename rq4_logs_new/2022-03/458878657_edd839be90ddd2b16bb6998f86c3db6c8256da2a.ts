import EngineInterface from '../car/details/engine/EngineInterface';

export default interface BuilderInterface {
  reset(): void;
  setSeats(seats: number): void;
  setEngine(engine: EngineInterface): void;
  setGps(gps: boolean): void;
}