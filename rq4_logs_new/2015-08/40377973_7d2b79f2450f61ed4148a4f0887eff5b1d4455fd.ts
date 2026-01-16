
export class BaseModel<TResponse> {

  constructor(properties:TResponse) {
    this.setProperties(properties);
  }

  setProperties(properties:TResponse):void {
    if (!properties) return;
  }

  getProperties(fromObject:any = this):TResponse {
    throw new Error('not implemented');
  }

}