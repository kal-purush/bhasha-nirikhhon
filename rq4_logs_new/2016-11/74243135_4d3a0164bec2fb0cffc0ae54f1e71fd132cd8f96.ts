import
{
    fakeAsync,
    inject,
    TestBed,
    async
} from '@angular/core/testing';
import
{
    HttpModule,
    XHRBackend,
    ResponseOptions,
    Response,
    RequestMethod,
    Http,
    Headers,
    Request,
    ConnectionBackend,
} from '@angular/http';
import
{
    MockBackend,
    MockConnection
} from '@angular/http/testing/mock_backend';

/* from HttpModule */
import { RequestOptions } from '@angular/http';


export function httpFactory(xhrBackend, requestOptions)
{
    return new SecuredHttpService(xhrBackend, requestOptions);
}

/**
 * Unit test for SecuredHttp
 */
import { SecuredHttpService } from './secured-http.service';

describe('Secured Http', () =>
{

    const expectedToken: string = "token-abc";
    let serverCalled: boolean;

    beforeEach(() =>
    {
        TestBed.configureTestingModule({
            imports: [HttpModule],
            providers: [
                {
                    provide: Http,
                    useFactory: httpFactory,
                    deps: [XHRBackend, RequestOptions]
                },
                {
                    provide: XHRBackend,
                    useClass: MockBackend
                },
                {
                    provide: XHRBackend,
                    useClass: MockBackend
                },
                {
                    provide: ConnectionBackend,
                    useClass: MockBackend
                }
            ]
        });

        serverCalled = false;
    });

    it(`should add authoriation token to header request without options`, fakeAsync(
        inject([
            XHRBackend,
            Http
        ], (mockBackend: MockBackend, securedHttpService: SecuredHttpService) =>
            {
                mockBackend.connections.subscribe(
                    (connection: MockConnection) =>
                    {
                        serverCalled = true;

                        expect(connection.request.headers.get('Authorization')).toBe(expectedToken);
                        connection.mockRespond(new Response(new ResponseOptions({ body: ['data'] })));
                    });

                securedHttpService.authorizationToken = expectedToken;
                securedHttpService.get("http://no-url-needed");

                expect(serverCalled).toEqual(true);
            })
    ));

    it(`should add authoriation token to header request with options but no header`, fakeAsync(
        inject([
            XHRBackend,
            Http
        ], (mockBackend: MockBackend, securedHttpService: SecuredHttpService) =>
            {
                mockBackend.connections.subscribe(
                    (connection: MockConnection) =>
                    {
                        serverCalled = true;

                        expect(connection.request.headers.get('Authorization')).toBe(expectedToken);
                        connection.mockRespond(new Response(new ResponseOptions({ body: ['data'] })));
                    });

                securedHttpService.authorizationToken = expectedToken;
                securedHttpService.get("http://no-url-needed", new RequestOptions({}));

                expect(serverCalled).toEqual(true);
            })
    ));

    it(`should add authoriation token to header request with options but and header`, fakeAsync(
        inject([
            XHRBackend,
            Http
        ], (mockBackend: MockBackend, securedHttpService: SecuredHttpService) =>
            {
                let originalHeaderValue: string = 'header-value-1';

                mockBackend.connections.subscribe(
                    (connection: MockConnection) =>
                    {
                        serverCalled = true;

                        expect(connection.request.headers.get('Authorization')).toBe(expectedToken);
                        expect(connection.request.headers.get('headerKey')).toBe(originalHeaderValue);

                        connection.mockRespond(new Response(new ResponseOptions({ body: ['data'] })));
                    });

                securedHttpService.authorizationToken = expectedToken;
                let headers = new Headers({ headerKey: originalHeaderValue });

                securedHttpService.get("http://no-url-needed", new RequestOptions({ headers: headers }));

                expect(serverCalled).toEqual(true);
            })
    ));
    it(`should fail when calling request method directly with a string url`, fakeAsync(
        inject([
            XHRBackend,
            Http
        ], (mockBackend: MockBackend, securedHttpService: Http) =>
            {
                expect(() => securedHttpService.request("http://no-url-needed")).toThrowError();
            })
    ));

});