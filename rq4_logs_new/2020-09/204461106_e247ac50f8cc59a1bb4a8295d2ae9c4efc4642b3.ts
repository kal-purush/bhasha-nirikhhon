/**
 * Metric collection handler.
 *
 * @class Metrics
 */
class Metrics {
  private tiles: number
  private clicks: number
  private selected: number
  private geocoding: number
  private startTime: Date
  private endTime: Date
  /**
   *Creates an instance of Metrics.
   *
   * @memberof Metrics
   */
  constructor() {
    this.reset()
  }

  /**
   *Increments the geocoding value by one.
   *
   * @memberof Metrics
   */
  public addGeocoding(): void {
    this.geocoding++
  }

  /**
   *Increments the tiles value by one.
   *
   * @memberof Metrics
   */
  public addtileLoad(): void {
    this.tiles++
  }

  /**
   *Increments the clicks value by one.
   *
   * @memberof Metrics
   */
  public addClick(): void {
    this.clicks++
  }

  /**
   *Increments the selected value by one.
   *
   * @memberof Metrics
   */
  public addSelect(): void {
    this.selected++
  }

  /**
   * Initializes / resets all values to 0.
   *
   * @memberof Metrics
   */
  public reset(): void {
    this.tiles = 0
    this.geocoding = 0
    this.selected = 0
    this.clicks = 0
    this.startTime = new Date()
  }

  /**
   * Sets the end time.
   *
   * @memberof Metrics
   */
  public stop(): void {
    this.endTime = new Date()
  }

  /**
   * Returns the collected metrics.
   *
   * @memberof Metrics
   * @returns Serialized JSON.
   */
  public getResult(): string {
    return JSON.stringify(
      {
        tiles: this.tiles,
        clicks: this.clicks,
        geocoding: this.geocoding,
        selected: this.selected,
        start: this.startTime.toLocaleTimeString(),
        end: this.endTime.toLocaleTimeString(),
      },
      null,
      2,
    )
  }
}

export const metrics = new Metrics()