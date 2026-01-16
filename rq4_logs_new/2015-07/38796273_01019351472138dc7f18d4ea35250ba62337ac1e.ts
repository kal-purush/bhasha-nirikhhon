class ScheduleType {

  protected id: string;
  protected markOverlap: boolean = false;
  public name: string;
  public height: number = 24;
  public hasLabel: boolean = false;
  public opacity: number = 0.9;
  public backgroundColor: string = "#ccffcc";
  public foregroundColor: string = "#447744";
  public zIndex: number = 0;
  public round: number = 0;
  public stroke: string = "";
  public strokeWidth: number = 0;
  public fontSize: number = 11;
  public overlapClass: string = "overlapping";

  protected allowedBatch: Array<string> = [
    "backgroundColor", "foregroundColor", "stroke", "strokeWidth", "fontSize",
    "round", "zIndex", "opacity", "height", "markOverlap", "hasLabel", "name",
    "overlapClass"
  ];

  public constructor(id: string, options ?: Object) {
    this.setId(id);
    if (options) {
      this.setOptions(options);
    }
  }

  public setOptions(options: Object): ScheduleType {
    if (options) {
      for (var key in options) {
        var value: any = options[key];
        if (this.hasOwnProperty(key) && this.allowedBatch.indexOf(key) >= 0) {
          this[key] = value;
        }
      }
    }
    return this;
  }

  public setId(id: string): ScheduleType {
    this.id = id;
    return this;
  }

  public setName(name: string): ScheduleType {
    this.name = name;
    return this;
  }

  public enableOverlapping(): ScheduleType {
    this.markOverlap = true;
    return this;
  }

  public disableOverlapping(): ScheduleType {
    this.markOverlap = false;
    return this;
  }

  public enableLabeling(): ScheduleType {
    this.hasLabel = true;
    return this;
  }

  public disableLabeling(): ScheduleType {
    this.hasLabel = false;
    return this;
  }
}