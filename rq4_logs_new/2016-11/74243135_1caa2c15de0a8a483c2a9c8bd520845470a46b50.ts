import { fakeAsync, inject, TestBed } from '@angular/core/testing';
import { MockBackend, MockConnection } from '@angular/http/testing/mock_backend';
import { LoginService } from '../login/login.service';
import { Http, XHRBackend, ConnectionBackend, RequestOptions, BaseRequestOptions, Response, ResponseOptions } from '@angular/http';
import { SecuredHttp } from '../login/secured-http';
import { SecuredHttpService } from '../http/secured-http.service';

export function httpFactory(xhrBackend, requestOptions) {
  return new SecuredHttpService(xhrBackend, requestOptions);
}

describe('web-music-player App', function () {

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        LoginService,
        {
          provide: SecuredHttp,
          useFactory: httpFactory,
          deps: [XHRBackend, RequestOptions]
        },
        {
          provide: Http,
          useFactory: httpFactory,
          deps: [XHRBackend, RequestOptions]
        },
        {
          provide: ConnectionBackend,
          useClass: MockBackend
        },
        {
          provide: RequestOptions,
          useClass: BaseRequestOptions
        },
        {
          provide: XHRBackend,
          useClass: MockBackend
        }
      ]
    })
  });

  it('should set header correctly to all subsequent http calls after login', fakeAsync(
    inject([
      XHRBackend,
      Http,
      LoginService,
    ], (mockBackEnd: MockBackend, http: Http, loginService: LoginService) => {

      let expectedToken: string = "logged-in-token";
      let authoriozationSet: boolean = false;
      let actualToken: string;

      //Backend mock
      mockBackEnd.connections.subscribe(
        (connection: MockConnection) => {
          if (authoriozationSet) {
            // Login completed. Save authorization header for check later. 
            actualToken = connection.request.headers.get('Authorization');
            return;
          }

          // Login not completed. Mock successful login reponse from server.
          let reponse = [
            { token: expectedToken }
          ];

          connection.mockRespond(new Response(new ResponseOptions({ body: reponse })));
        });

      //Perform login
      loginService.login(
        "TestUser",
        "xxx",
        () => { authoriozationSet = true },
        () => { throw new Error("Error on login") });

      //Perform a http request
      http.get('http://any-url.ca');

      //Verify Authorization header was set as expected
      expect(actualToken).toEqual(expectedToken);
    }
    )));
});