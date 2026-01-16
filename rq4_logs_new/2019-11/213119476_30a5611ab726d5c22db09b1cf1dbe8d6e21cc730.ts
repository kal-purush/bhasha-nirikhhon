import RequestHandler from '../request/handler';

export default abstract class Collection {
  protected requestHandler: RequestHandler;

  constructor(requestHandler: RequestHandler) {
    this.requestHandler = requestHandler;
  }

  protected async createAndSendRequest(
    pathname: string,
    {
      query,
      method,
      body
    }: {
      query?: Partial<{ [key: string]: string }>;
      method?: string;
      body?: any;
    } = {}
  ): Promise<any> {
    return await this.requestHandler.doRequest(
      this.requestHandler.makeRequestHeader(
        this.requestHandler.makeUri({
          pathname,
          query
        }),
        {
          body,
          method
        }
      )
    );
  }
}