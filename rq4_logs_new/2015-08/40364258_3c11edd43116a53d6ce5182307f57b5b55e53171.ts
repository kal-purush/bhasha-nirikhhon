
export class InformationsService {
  applicationName: string = "NG2-discovery";
  version: string = "01-services";

  constructor() {}

  getApplicationName() {
    return this.applicationName;
  }

  getVersion() {
    return this.version;
  }
}