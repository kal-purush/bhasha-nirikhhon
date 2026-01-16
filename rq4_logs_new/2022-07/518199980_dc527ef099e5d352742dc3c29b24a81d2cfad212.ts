import { IClientOptions } from "./IClientOptions";

class AuthenticationClient {
  /** Variable to hold different components of the URL */
  private urlComponents: Array<string>;

  /** Configuration to initialize the client with */
  private config: IClientOptions;

  private constructor(clientOptions: IClientOptions) {
    this.urlComponents = ["Main"];
    this.config = clientOptions;
  }

  /** Initialize the client through its private constructor */
  public static init(options: IClientOptions) {
    const clientOptions: IClientOptions = {};
    for (const i in options) {
      if (Object.prototype.hasOwnProperty.call(options, i)) {
        clientOptions[i as keyof IClientOptions] =
          options[i as keyof IClientOptions];
      }
    }
    return new AuthenticationClient(options);
  }
}

export { AuthenticationClient };