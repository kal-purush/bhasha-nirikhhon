export class Score {

  private static _instance: Score = new Score();
  private _score: number = 0;

  constructor() {
    if (Score._instance)
      throw new Error('Instantiation of Service failed: ' +
        'Use Service.getInstance() instead of new Service()');
  }

  public static getInstance(): Score {
    return Score._instance;
  }

  set score(value: number) {
    this._score = value;
  }

  get score(): number {
    return this._score;
  }

  public addPoints(value: number): void {
    this._score += value;
  }

  public removePoints(value: number): void {
    this._score -= value;
  }

}