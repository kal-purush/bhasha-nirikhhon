/*
declare class TokenRequest {
  client_id: string;
  redirect_uri: string;
  grant_type: string;
  code?: string;
}

class TokenResponse {
  accessToken: string;
  tokenType: TokenType;
  expiresIn: number | undefined;
  refreshToken: string | undefined;
  scope: string | undefined;
  idToken: string | undefined;
  issuedAt: number;

  constructor(response: TokenResponseJson) {
    this.accessToken = response.access_token;
    this.tokenType = response.token_type || 'bearer';
    if (response.expires_in) {
      this.expiresIn = parseInt(response.expires_in, 10);
    }
    this.refreshToken = response.refresh_token;
    this.scope = response.scope;
    this.idToken = response.id_token;
    this.issuedAt = response.issued_at || nowInSeconds();
  }

  toJson(): TokenResponseJson {
    return {
      access_token: this.accessToken,
      id_token: this.idToken,
      refresh_token: this.refreshToken,
      scope: this.scope,
      token_type: this.tokenType,
      issued_at: this.issuedAt,
      expires_in: this.expiresIn?.toString()
    };
  }

  isValid(buffer: number = AUTH_EXPIRY_BUFFER): boolean {
    if (this.expiresIn) {
      let now = nowInSeconds();
      return now < this.issuedAt + this.expiresIn + buffer;
    }
    else {
      return true;
    }
  }
}

interface TokenResponseJson {
  access_token: string;
  token_type?: TokenType;
  expires_in?: string;
  refresh_token?: string;
  scope?: string;
  id_token?: string;
  issued_at?: number;
}
const AUTH_EXPIRY_BUFFER = 10 //60 //-1;  // 10 mins in seconds
const nowInSeconds = () => Math.round(new Date().getTime() / 1000);
declare type TokenType = 'bearer' | 'mac';
declare type ErrorType = 'invalid_request' | 'invalid_client' | 'invalid_grant' | 'unauthorized_client' | 'unsupported_grant_type' | 'invalid_scope';
interface AuthorizationServiceConfigurationJson {
  authorization_endpoint: string;
  token_endpoint: string;
  revocation_endpoint: string;
  end_session_endpoint?: string;
  userinfo_endpoint?: string;
}

declare class AuthorizationServiceConfiguration {
  authorizationEndpoint: string;
  tokenEndpoint: string;
  revocationEndpoint: string;
  userInfoEndpoint?: string;
  endSessionEndpoint?: string;
  constructor(request: AuthorizationServiceConfigurationJson);
  toJson(): {
    authorization_endpoint: string;
    token_endpoint: string;
    revocation_endpoint: string;
    end_session_endpoint: string | undefined;
    userinfo_endpoint: string | undefined;
  };
  static fetchFromIssuer(openIdIssuerUrl: string, requestor?: XMLHttpRequest): Promise<AuthorizationServiceConfiguration>;
}

class AppAuthError {
  constructor(public message: string, public extras?: any) { }
}


const SIZE = 10;  // 10 bytes
const newState = function (crypto: Crypto): string {
  return crypto.generateRandom(SIZE);
};

const HAS_CRYPTO = typeof window !== 'undefined' && !!(window.crypto as any);
const HAS_SUBTLE_CRYPTO = HAS_CRYPTO && !!(window.crypto.subtle as any);
const CHARSET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';

function bufferToString(buffer: Uint8Array) {
  let state = [];
  for (let i = 0; i < buffer.byteLength; i += 1) {
    let index = buffer[i] % CHARSET.length;
    state.push(CHARSET[index]);
  }
  return state.join('');
}

function textEncodeLite(str: string) {
  const buf = new ArrayBuffer(str.length);
  const bufView = new Uint8Array(buf);

  for (let i = 0; i < str.length; i++) {
    bufView[i] = str.charCodeAt(i);
  }
  return bufView;
}

export interface Crypto {
  //
   //Generate a random string
   //
  generateRandom(size: number): string;
  //
   //Compute the SHA256 of a given code.
   //This is useful when using PKCE.
   //
  // deriveChallenge(code: string): Promise<string>;
}

//
 //The default implementation of the `Crypto` interface.
 //This uses the capabilities of the browser.
 //
export class DefaultCrypto implements Crypto {
  generateRandom(size: number) {
    const buffer = new Uint8Array(size);
    if (HAS_CRYPTO) {
      window.crypto.getRandomValues(buffer);
    } else {
      // fall back to Math.random() if nothing else is available
      for (let i = 0; i < size; i += 1) {
        buffer[i] = (Math.random() * CHARSET.length) | 0;
      }
    }
    return bufferToString(buffer);
  }

  // deriveChallenge(code: string): Promise<string> {
  //   if (code.length < 43 || code.length > 128) {
  //     return Promise.reject(new AppAuthError('Invalid code length.'));
  //   }
  //   if (!HAS_SUBTLE_CRYPTO) {
  //     return Promise.reject(new AppAuthError('window.crypto.subtle is unavailable.'));
  //   }

  //   return new Promise((resolve, reject) => {
  //     crypto.subtle.digest('SHA-256', textEncodeLite(code)).then(buffer => {
  //       return resolve(urlSafe(new Uint8Array(buffer)));
  //     }, error => reject(error));
  //   });
  // }
}

interface AuthorizationRequestJson {
  response_type: string;
  client_id: string;
  redirect_uri: string;
  scope: string;
  state?: string;
  extras?: StringMap;
  internal?: StringMap;
}

//
 //Represents the AuthorizationRequest.
 //For more information look at
 //https://tools.ietf.org/html/rfc6749#section-4.1.1
 //
class AuthorizationRequest {
  static RESPONSE_TYPE_TOKEN = 'token';
  static RESPONSE_TYPE_CODE = 'code';

  // NOTE:
  // Both redirect_uri and state are actually optional.
  // However AppAuth is more opionionated, and requires you to use both.

  clientId: string;
  redirectUri: string;
  scope: string;
  responseType: string;
  state: string;
  extras?: StringMap;
  internal?: StringMap;
  //
   //Constructs a new AuthorizationRequest.
   //Use a `undefined` value for the `state` parameter, to generate a random
   //state for CSRF protection.
   //
  constructor(
    request: AuthorizationRequestJson,
    private crypto: Crypto = new DefaultCrypto(),
    private usePkce: boolean = true) {
    this.clientId = request.client_id;
    this.redirectUri = request.redirect_uri;
    this.scope = request.scope;
    this.responseType = request.response_type || AuthorizationRequest.RESPONSE_TYPE_CODE;
    this.state = request.state || newState(crypto);
    this.extras = request.extras;
    // read internal properties if available
    this.internal = request.internal;
  }

  setupCodeVerifier(): Promise<void> {
    return Promise.resolve();
    // if (!this.usePkce) {
    //   return Promise.resolve();
    // } else {
    //   const codeVerifier = this.crypto.generateRandom(128);
    //   const challenge: Promise<string|undefined> =
    //       this.crypto.deriveChallenge(codeVerifier).catch(error => {
    //         // log('Unable to generate PKCE challenge. Not using PKCE', error);
    //         return undefined;
    //       });
    //   return challenge.then(result => {
    //     if (result) {
    //       // keep track of the code used.
    //       this.internal = this.internal || {};
    //       this.internal['code_verifier'] = codeVerifier;
    //       this.extras = this.extras || {};
    //       this.extras['code_challenge'] = result;
    //       // We always use S256. Plain is not good enough.
    //       this.extras['code_challenge_method'] = 'S256';
    //     }
    //   });
    // }
  }

  //
   //Serializes the AuthorizationRequest to a JavaScript Object.
   //
  toJson(): Promise<AuthorizationRequestJson> {
    // Always make sure that the code verifier is setup when toJson() is called.
    return this.setupCodeVerifier().then(() => {
      return {
        response_type: this.responseType,
        client_id: this.clientId,
        redirect_uri: this.redirectUri,
        scope: this.scope,
        state: this.state,
        extras: this.extras,
        internal: this.internal
      };
    });
  }
}

interface StringMap {
  [key: string]: string;
}

//
 //Represents a window.location like object.
 //
interface LocationLike {
  hash: string;
  host: string;
  origin: string;
  hostname: string;
  pathname: string;
  port: string;
  protocol: string;
  search: string;
  assign(url: string): void;
}

//
 //Query String Utilities.
 //
interface QueryStringUtils {
  stringify(input: StringMap): string;
  parse(query: LocationLike, useHash?: boolean): StringMap;
  parseQueryString(query: string): StringMap;
}

class BasicQueryStringUtils implements QueryStringUtils {
  parse(input: LocationLike, useHash?: boolean) {
    if (useHash) {
      return this.parseQueryString(input.hash);
    } else {
      return this.parseQueryString(input.search);
    }
  }

  parseQueryString(query: string): StringMap {
    let result: StringMap = {};
    // if anything starts with ?, # or & remove it
    query = query.trim().replace(/^(\?|#|&)/, '');
    let params = query.split('&');
    for (let i = 0; i < params.length; i += 1) {
      let param = params[i];  // looks something like a=b
      let parts = param.split('=');
      if (parts.length >= 2) {
        let key = decodeURIComponent(parts.shift()!);
        let value = parts.length > 0 ? parts.join('=') : null;
        if (value) {
          result[key] = decodeURIComponent(value);
        }
      }
    }
    return result;
  }

  stringify(input: StringMap) {
    let encoded: string[] = [];
    for (let key in input) {
      if (input.hasOwnProperty(key) && input[key]) {
        encoded.push(`${encodeURIComponent(key)}=${encodeURIComponent(input[key])}`)
      }
    }
    return encoded.join('&');
  }
}

//
//Defines the interface which is capable of handling an authorization request
//using various methods (iframe / popup / different process etc.).
//
abstract class AuthorizationRequestHandler {
  constructor(public utils: QueryStringUtils, protected crypto: Crypto) { }

  // notifier send the response back to the client.
  protected notifier: AuthorizationNotifier | null = null;

  //
  //A utility method to be able to build the authorization request URL.
  //
  protected buildRequestUrl(
    configuration: AuthorizationServiceConfiguration,
    request: AuthorizationRequest) {
    // build the query string
    // coerce to any type for convenience
    let requestMap: StringMap = {
      'redirect_uri': request.redirectUri,
      'client_id': request.clientId,
      'response_type': request.responseType,
      'state': request.state,
      'scope': request.scope
    };

    // copy over extras
    // if (request.extras) {
    //   for (let extra in request.extras) {
    //     if (request.extras.hasOwnProperty(extra)) {
    //       // check before inserting to requestMap
    //       if (BUILT_IN_PARAMETERS.indexOf(extra) < 0) {
    //         requestMap[extra] = request.extras[extra];
    //       }
    //     }
    //   }
    // }

    let query = this.utils.stringify(requestMap);
    let baseUrl = configuration.authorizationEndpoint;
    let url = `${baseUrl}?${query}`;
    return url;
  }

  //
  //Completes the authorization request if necessary & when possible.
  //
  completeAuthorizationRequestIfPossible(): Promise<void> {
    // call complete authorization if possible to see there might
    // be a response that needs to be delivered.
    //log(`Checking to see if there is an authorization response to be delivered.`);
    if (!this.notifier) {
      //log(`Notifier is not present on AuthorizationRequest handler.No delivery of result will be possible`)
    }
    return this.completeAuthorizationRequest().then(result => {
      if (!result) {
        //log(`No result is available yet.`);
      }
      if (result && this.notifier) {
        this.notifier.onAuthorizationComplete(result.request, result.response, result.error);
      }
    });
  }

  //
  //Sets the default Authorization Service notifier.
  //
  setAuthorizationNotifier(notifier: AuthorizationNotifier): AuthorizationRequestHandler {
    this.notifier = notifier;
    return this;
  };

  //
  //Makes an authorization request.
  //
  abstract performAuthorizationRequest(
    configuration: AuthorizationServiceConfiguration,
    request: AuthorizationRequest): void;

  //
  //Checks if an authorization flow can be completed, and completes it.
  //The handler returns a `Promise<AuthorizationRequestResponse>` if ready, or a `Promise<null>`
  //if not ready.
  //
  protected abstract completeAuthorizationRequest(): Promise<AuthorizationRequestResponse | null>;
}

interface UnderlyingStorage {
  readonly length: number;
  clear(): void;
  getItem(key: string): string | null;
  removeItem(key: string): void;
  setItem(key: string, data: string): void;
}

//
 //Asynchronous storage APIs. All methods return a `Promise`.
 //All methods take the `DOMString`
 //IDL type (as it is the lowest common denominator).
 //
abstract class StorageBackend {
  //
   //When passed a key `name`, will return that key's value.
   //
  public abstract getItem(name: string): Promise<string | null>;

  //
   //When passed a key `name`, will remove that key from the storage.
   //
  public abstract removeItem(name: string): Promise<void>;

  //
   //When invoked, will empty all keys out of the storage.
   //
  public abstract clear(): Promise<void>;

  //
   //The setItem() method of the `StorageBackend` interface,
   //when passed a key name and value, will add that key to the storage,
   //or update that key's value if it already exists.
   //
  public abstract setItem(name: string, value: string): Promise<void>;
}

//
 //A `StorageBackend` backed by `localstorage`.
 //
class LocalStorageBackend extends StorageBackend {
  private storage: UnderlyingStorage;
  constructor(storage?: UnderlyingStorage) {
    super();
    this.storage = storage || window.localStorage;
  }

  public getItem(name: string): Promise<string | null> {
    return new Promise<string | null>((resolve, reject) => {
      const value = this.storage.getItem(name);
      if (value) {
        resolve(value);
      } else {
        resolve(null);
      }
    });
  }

  public removeItem(name: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.storage.removeItem(name);
      resolve();
    });
  }

  public clear(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.storage.clear();
      resolve();
    });
  }

  public setItem(name: string, value: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.storage.setItem(name, value);
      resolve();
    });
  }
}

//
 //Represents the AuthorizationResponse as a JSON object.
 //
interface AuthorizationResponseJson {
  code: string;
  state: string;
}

//
 //Represents the AuthorizationError as a JSON object.
 //
interface AuthorizationErrorJson {
  error: string;
  error_description?: string;
  error_uri?: string;
  state?: string;
}

//
 //Represents the Authorization Response type.
 //For more information look at
 //https://tools.ietf.org/html/rfc6749#section-4.1.2
 //
class AuthorizationResponse {
  code: string;
  state: string;

  constructor(response: AuthorizationResponseJson) {
    this.code = response.code;
    this.state = response.state;
  }

  toJson(): AuthorizationResponseJson {
    return { code: this.code, state: this.state };
  }
}

//
 //Represents the Authorization error response.
 //For more information look at:
 //https://tools.ietf.org/html/rfc6749#section-4.1.2.1
 //
class AuthorizationError {
  error: string;
  errorDescription?: string;
  errorUri?: string;
  state?: string;

  constructor(error: AuthorizationErrorJson) {
    this.error = error.error;
    this.errorDescription = error.error_description;
    this.errorUri = error.error_uri;
    this.state = error.state;
  }

  toJson(): AuthorizationErrorJson {
    return {
      error: this.error,
      error_description: this.errorDescription,
      error_uri: this.errorUri,
      state: this.state
    };
  }
}

// key for authorization request. //
const authorizationRequestKey =
  (handle: string) => {
    return `${handle}_appauth_authorization_request`;
  }

// key for authorization service configuration //
const authorizationServiceConfigurationKey =
  (handle: string) => {
    return `${handle}_appauth_authorization_service_configuration`;
  }

// key in local storage which represents the current authorization request. //
const AUTHORIZATION_REQUEST_HANDLE_KEY = 'appauth_current_authorization_request';

//
 //Represents an AuthorizationRequestHandler which uses a standard
 //redirect based code flow.
 //
class RedirectRequestHandler extends AuthorizationRequestHandler {
  constructor(
    // use the provided storage backend
    // or initialize local storage with the default storage backend which
    // uses window.localStorage
    public storageBackend: StorageBackend = new LocalStorageBackend(),
    utils = new BasicQueryStringUtils(),
    public locationLike: LocationLike = window.location,
    crypto: Crypto = new DefaultCrypto()) {
    super(utils, crypto);
  }

  performAuthorizationRequest(
    configuration: AuthorizationServiceConfiguration,
    request: AuthorizationRequest) {
    const handle = this.crypto.generateRandom(10);

    // before you make request, persist all request related data in local storage.
    const persisted = Promise.all([
      this.storageBackend.setItem(AUTHORIZATION_REQUEST_HANDLE_KEY, handle),
      // Calling toJson() adds in the code & challenge when possible
      request.toJson().then(result => {
        this.storageBackend.setItem(authorizationRequestKey(handle), JSON.stringify(result))
      }),
      this.storageBackend.setItem(authorizationServiceConfigurationKey(handle), JSON.stringify(configuration.toJson())),
    ]);

    persisted.then(() => {
      // make the redirect request
      let url = this.buildRequestUrl(configuration, request);
      // log('Making a request to ', request, url);
      this.locationLike.assign(url);
    });
  }

  //
   //Attempts to introspect the contents of storage backend and completes the
   //request.
   //
  protected completeAuthorizationRequest(): Promise<AuthorizationRequestResponse | null> {
    // TODO(rahulrav@): handle authorization errors.
    return this.storageBackend.getItem(AUTHORIZATION_REQUEST_HANDLE_KEY).then(handle => {
      if (handle) {
        // we have a pending request.
        // fetch authorization request, and check state
        return this.storageBackend
          .getItem(authorizationRequestKey(handle))
          // requires a corresponding instance of result
          // TODO(rahulrav@): check for inconsitent state here
          .then(result => JSON.parse(result!))
          .then(json => new AuthorizationRequest(json))
          .then(request => {
            // check redirect_uri and state
            let currentUri = `${this.locationLike.origin}${this.locationLike.pathname}`;
            let queryParams = this.utils.parse(this.locationLike, true) ///use hash //);
            let state: string | undefined = queryParams['state'];
            let code: string | undefined = queryParams['code'];
            let error: string | undefined = queryParams['error'];
            // log('Potential authorization request ', currentUri, queryParams, state, code, error);
            let shouldNotify = state === request.state;
            let authorizationResponse: AuthorizationResponse | null = null;
            let authorizationError: AuthorizationError | null = null;
            if (shouldNotify) {
              if (error) {
                // get additional optional info.
                let errorUri = queryParams['error_uri'];
                let errorDescription = queryParams['error_description'];
                authorizationError = new AuthorizationError({
                  error: error,
                  error_description: errorDescription,
                  error_uri: errorUri,
                  state: state
                });
              } else {
                authorizationResponse = new AuthorizationResponse({ code: code, state: state });
              }
              // cleanup state
              return Promise
                .all([
                  this.storageBackend.removeItem(AUTHORIZATION_REQUEST_HANDLE_KEY),
                  this.storageBackend.removeItem(authorizationRequestKey(handle)),
                  this.storageBackend.removeItem(authorizationServiceConfigurationKey(handle))
                ])
                .then(() => {
                  // log('Delivering authorization response');
                  return {
                    request: request,
                    response: authorizationResponse,
                    error: authorizationError
                  } as AuthorizationRequestResponse;
                });
            } else {
              // log('Mismatched request (state and request_uri) dont match.');
              return Promise.resolve(null);
            }
          });
      } else {
        return null;
      }
    });
  }
}

//
 //This type represents a lambda that can take an AuthorizationRequest,
 //and an AuthorizationResponse as arguments.
 //
type AuthorizationListener =
  (request: AuthorizationRequest,
    response: AuthorizationResponse | null,
    error: AuthorizationError | null) => void;

//
//Represents a structural type holding both authorization request and response.
//
interface AuthorizationRequestResponse {
  request: AuthorizationRequest;
  response: AuthorizationResponse | null;
  error: AuthorizationError | null;
}

//
//Authorization Service notifier.
//This manages the communication of the AuthorizationResponse to the 3p client.
//
class AuthorizationNotifier {
  private listener: AuthorizationListener | null = null;

  setAuthorizationListener(listener: AuthorizationListener) {
    this.listener = listener;
  }

  //
  //The authorization complete callback.
  //
  onAuthorizationComplete(
    request: AuthorizationRequest,
    response: AuthorizationResponse | null,
    error: AuthorizationError | null): void {
    if (this.listener) {
      // complete authorization request
      this.listener(request, response, error);
    }
  }
}

///built in parameters. //
const BUILT_IN_PARAMETERS = ['redirect_uri', 'client_id', 'response_type', 'state', 'scope'];

class OAuthAuthentication {
  private readonly _authorizationServiceConfiguration: AuthorizationServiceConfiguration;
  private readonly _redirectRequestHandler: RedirectRequestHandler = new RedirectRequestHandler;
  private readonly _issuerEndpoint: string;
  constructor(protected readonly issuerEndpoint: string, protected readonly authorizationEndpoint: string, protected readonly tokenEndpoint: string, protected readonly revocationEndpoint: string) {
    this._issuerEndpoint = issuerEndpoint;
    this._authorizationServiceConfiguration = new AuthorizationServiceConfiguration({
      authorization_endpoint: authorizationEndpoint,
      token_endpoint: tokenEndpoint,
      revocation_endpoint: revocationEndpoint
    });
  }

  public async Authorize(clientId: string, scopes: string[]): Promise<TokenResponse> {
    const redirectUrl = new URL(window.location.toString());
    let authorizationRequest = new AuthorizationRequest({
      client_id: clientId,
      redirect_uri: redirectUrl.toString(),
      scope: scopes.join(' '),
      response_type: AuthorizationRequest.RESPONSE_TYPE_CODE
    });
    // await authorizationRequest.setupCodeVerifier(); // use pkce
    this._redirectRequestHandler.performAuthorizationRequest(this._authorizationServiceConfiguration, authorizationRequest);
    return await this.GetToken(clientId);
  }

  private async GetToken(clientId: string): Promise<TokenResponse> {
    const authorizationNotifier = new AuthorizationNotifier();
    this._redirectRequestHandler.setAuthorizationNotifier(authorizationNotifier);

    await this._redirectRequestHandler.completeAuthorizationRequestIfPossible();
    let tokenRequest: any;
    authorizationNotifier.setAuthorizationListener((request, response, error) => {
      // response object returns code which is in URL i.e. response.code
      // request object returns code_verifier i.e request.internal.code_verifier
      // you will need to add here token request process

      // Exchange for an access token and return tokenResponse
      tokenRequest = new TokenRequest();
      tokenRequest.client_id = clientId;
      tokenRequest.redirect_uri = this._issuerEndpoint;
      tokenRequest.grant_type = 'authorization_code';
      tokenRequest.code = response?.code;
    });
    return await this.RequestToken(this._authorizationServiceConfiguration, tokenRequest);
  }

  private async RequestToken(configuration: AuthorizationServiceConfiguration, request: TokenRequest): Promise<TokenResponse> {
    let headers: [string, string][] = [];
    headers.push(['Content-Type', 'application/json; charset=utf-8']);
    headers.push(['X-Requested-With', 'XMLHttpRequest']);
    headers.push(['Cache-Control', 'no-cache, no-store, must-revalidate']);

    return new Promise<TokenResponse>(
      (resolve, reject) => {
        const xhr: XMLHttpRequest = new XMLHttpRequest();
        xhr.withCredentials = true;
        xhr.onload = () => {
          const reponse = xhr.responseText;
          //resolve()
        };
        for (let i: number = 0; i < headers.length; i++) {
          const header: [string, string] = headers[i];
          xhr.setRequestHeader(header[0], this.EnsureASCII(header[1]));
        }
        xhr.open('POST', configuration.tokenEndpoint, true);
        xhr.send(JSON.stringify(request));
      }
    );
  }

  private EnsureASCII(data: string): string {
    if (this.HasUnicode(data))
      return (this.ConvertToASCII(data));
    return (data);
  }

  private HasUnicode(data: string): boolean {
    for (let i = 0; i < data.length; i++) {
      const char: string = data[i];
      const index: number = char.charCodeAt(0);
      if (index > 127)
        return (true);
    }
    return (false);
  }

  private ConvertToASCII(data: string): string {
    let encoded: string = '';
    for (let i = 0; i < data.length; i++) {
      const char: string = data[i];
      const index: number = char.charCodeAt(0);
      encoded += '\\u' + index.toString(16).toUpperCase();
    }
    return (encoded);
  }
}
*/