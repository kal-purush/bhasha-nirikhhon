import RequestHandler from '../request/handler';

export default class WorkAttributes {
  private requestHandler: RequestHandler;

  constructor(requestHandler: RequestHandler) {
    this.requestHandler = requestHandler;
  }

  public async get() {
    return await this.requestHandler.doRequest(
      this.requestHandler.makeRequestHeader(
        this.requestHandler.makeUri({
          pathname: `/work-attributes`
        })
      )
    );
  }

  public async getWorkAttribute(key: string) {
    return await this.requestHandler.doRequest(
      this.requestHandler.makeRequestHeader(
        this.requestHandler.makeUri({
          pathname: `/work-attributes/${key}`
        })
      )
    );
  }
}