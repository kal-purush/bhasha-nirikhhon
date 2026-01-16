import * as request from 'request-promise';
import * as url from 'url';

interface IHandlerOptions {
  port?: string;
  request?: request.RequestPromiseAPI;
  timeout?: number;
  protocol: string;
  host: string;
  apiVersion: string;
  intermediatePath?: string;
  bearerToken?: string;
  strictSSL?: boolean;
}

interface IRequestHeaderOptions {
  method?: string;
}

interface IUriOptions {
  pathname: string;
  query?: Partial<{ [key: string]: string }>;
  intermediatePath?: string;
}

interface IRequestBaseOptions {
  timeout?: number;
  auth?: any;
}

export default class Handler {
  public readonly protocol: string;
  public readonly port?: string;
  public readonly host: string;
  public readonly apiVersion: string;
  public readonly bearerToken?: string;
  public readonly intermediatePath?: string;
  public readonly strictSSL: boolean;
  public readonly request: request.RequestPromiseAPI;
  public readonly baseOptions: {
    timeout?: number;
    auth?: any;
  };

  public constructor(options: IHandlerOptions) {
    this.protocol = options.protocol || 'https';
    this.intermediatePath = options.intermediatePath;
    this.strictSSL =
      options.hasOwnProperty('strictSSL') &&
      typeof options.strictSSL === 'boolean'
        ? options.strictSSL
        : true;
    this.port = options.port;
    this.host = options.host;
    this.apiVersion = options.apiVersion;
    this.bearerToken = options.bearerToken;
    this.request = options.request || request; // To mock requests
    this.baseOptions = {};

    if (this.bearerToken) {
      this.baseOptions.auth = {
        bearer: this.bearerToken,
        pass: '',
        sendImmediately: true,
        user: ''
      };
    }

    if (options.timeout) {
      this.baseOptions.timeout = options.timeout;
    }
  }

  public async doRequest(requestOptions: any) {
    const options = {
      ...this.baseOptions,
      ...requestOptions
    };

    const response = await this.request(options);

    if (response) {
      if (
        Array.isArray(response.errorMessages) &&
        response.errorMessages.length > 0
      ) {
        throw new Error(response.errorMessages.join(', '));
      }
    }

    return response;
  }

  public makeRequestHeader(uri: string, options: IRequestHeaderOptions = {}) {
    return {
      json: true,
      method: options.method || 'GET',
      rejectUnauthorized: this.strictSSL,
      uri,
      ...options
    };
  }

  public makeUri({ pathname, query, intermediatePath }: IUriOptions) {
    const intermediateToUse = this.intermediatePath || intermediatePath;
    const tempPath = intermediateToUse || `/core/${this.apiVersion}`;
    const uri = url.format({
      hostname: this.host,
      pathname: `${tempPath}${pathname}`,
      port: this.port,
      protocol: this.protocol,
      query
    });

    return decodeURIComponent(uri);
  }
}