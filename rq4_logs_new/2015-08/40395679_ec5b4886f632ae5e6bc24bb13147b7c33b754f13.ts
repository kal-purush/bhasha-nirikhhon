
export class InformationsService {
  applicationName: string = "Kiss loves Angular 2";
  version: string = "alpha";

  constructor() {}

  getApplicationName() {
    return this.applicationName;
  }

  getVersion() {
    return this.version;
  }
}