import { CocktailsList } from './../app/app.models';
import { HttpClientModule, HttpClient } from '@angular/common/http';
import {
  fakeAsync,
  async,
  inject,
  TestBed,
  tick
} from '@angular/core/testing';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/catch';
import 'rxjs/add/operator/do';

import {
  ResponseOptions,
  Response,
  RequestMethod,
  XHRBackend,
} from '@angular/http';

import {
  Http,
  BaseRequestOptions,
  RequestOptions,
  HttpModule,
  ConnectionBackend
} from '@angular/http';
import { pluck } from 'rxjs/operators';
import {
  MockBackend,
  MockConnection
} from '@angular/http/testing';

import { DATA_URL } from '../config/api';
import { CocktailsService, DATA_KEY } from './cocktails.service';

const makeCocktails = () => [{
  'strDrink': '57 Chevy with a White License Plate',
  'strDrinkThumb': 'www.thecocktaildb.com\/images\/media\/drink\/qyyvtu1468878544.jpg',
  'idDrink': '14029'
}, {
  'strDrink': '155 Belmont',
  'strDrinkThumb': 'www.thecocktaildb.com\/images\/media\/drink\/yqvvqs1475667388.jpg',
  'idDrink': '15346'
}] as CocktailsList;

describe('Cocktails get service', () => {
  beforeEach( async(() => {
    TestBed.configureTestingModule({
      imports: [ HttpClientModule ],
      providers: [
        CocktailsService,
        { provide: XHRBackend, useClass: MockBackend }
      ]
    })
    .compileComponents();
  }));

  it('can instantiate service when inject service',
    inject([CocktailsService], (service: CocktailsService) => {
      expect(service instanceof CocktailsService).toBe(true);
  }));

  it('can instantiate service with "new"', inject([HttpClient], (http: HttpClient) => {
    expect(http).not.toBeNull('http should be provided');
    const service = new CocktailsService(http);
    expect(service instanceof CocktailsService).toBe(true, 'new service should be ok');
  }));

  it('can provide the mockBackend as XHRBackend',
    inject([XHRBackend], (backend: MockBackend) => {
      expect(backend).not.toBeNull('backend should be provided');
  }));

  describe('when getCocktails', () => {
    let backend: MockBackend;
    let service: CocktailsService;
    let fakeCocktails: CocktailsList;
    let response: Response;

    beforeEach(inject([HttpClient, XHRBackend], (http: HttpClient, be: MockBackend) => {
      backend = be;
      service = new CocktailsService(http);
      fakeCocktails = makeCocktails();
      const options = new ResponseOptions({status: 200, body: {drinks: fakeCocktails}});
      response = new Response(options);
      console.log('$$$$response', response);
    }));

    xit('should have get URL params', async(inject([], () => {
      const expectedUrl = DATA_URL.COCKTAILS;
      // let request;
      backend.connections.subscribe((c: MockConnection) => c.mockRespond(response));
      service.getCocktails()
        .subscribe(() => {
          // console.log('request ===== ', request);
          // expect(request.method).toBe(RequestMethod.Get);
          // expect(request.url).toBe(expectedUrl);
        });

    })));

    xit('should have expected fake Cocktails (Observable)', async(inject([], () => {
      const resp = new Response(new ResponseOptions({status: 500, body: {drinks: makeCocktails()}}));
      backend.connections.subscribe((c: MockConnection) => c.mockRespond(resp));
      service.getCocktails()
        .subscribe((cocktails: CocktailsList) => {
          console.log('CocktailsList', cocktails.length);
          console.log('CocktailsList', fakeCocktails.length);
          expect(cocktails.length).toBe(fakeCocktails.length,
            'should have expected no. of Cocktails');
        });
    })));

    it('should be OK returning no Cocktails',  fakeAsync(inject([], () => {
      const resp = new Response(new ResponseOptions({status: 200, body: {drinks: []}}));
      backend.connections.subscribe((c: MockConnection) => c.mockRespond(resp));
      service.getCocktails()
        .subscribe((cocktails: CocktailsList) => {
          expect(cocktails.length).toBe(0, 'should have no heroes');
        });
    })));

    it('should treat 404 as an Observable error', async(inject([], () => {
      const resp = new Response(new ResponseOptions({status: 404}));
      backend.connections.subscribe((c: MockConnection) => c.mockRespond(resp));
      service.getCocktails()
        .catch(err => {
          expect(err).toMatch(/Something bad happened/, 'should catch bad response status code');
          return Observable.of(null); // failure is the expected test result
        })
        .do(data => {
          fail('should not respond with data');
        })
        .subscribe(data => data);
    })));


  });

/*
  xit('should get search results', fakeAsync(
    inject([
      ConnectionBackend,
      CocktailsService
    ], (mockBackend: MockBackend, mockService: CocktailsService) => {
      const expectedUrl = DATA_URL.COCKTAILS;
      const mockResponse = [];

      console.log('backend.connections', mockBackend.connections);

      mockBackend.connections.subscribe(
        (connection: MockConnection) => {
          expect(connection.request.method).toBe(RequestMethod.Get);
          expect(connection.request.url).toBe(expectedUrl);

          connection.mockRespond(new Response(
            new ResponseOptions({})
          ));

          tick();
        });
        console.log('service', mockService.getCocktails().subscribe);

        mockService.getCocktails().subscribe();
    })
  ));
  */
});
