import {provide, Injector} from '@angular/core';
import {describe, expect, it, xit, inject, async, fakeAsync, beforeEachProviders} from '@angular/core/testing';

import {
  ResponseOptions,
  Response,
  Http,
  BaseRequestOptions,
  RequestMethod
} from '@angular/http';

import { MockBackend, MockConnection } from '@angular/http/testing';

import {Observable} from 'rxjs/Rx';

import {MealSetXPath} from '../meal-set';

import {XpathResolutionResult, XpathService} from '../xpath';

import {Location, LocationJSON, Marker, MapService} from './index';

const mockHttpProvider = {
  deps: [ MockBackend, BaseRequestOptions ],
  useFactory: (backend: MockBackend, defaultOptions: BaseRequestOptions) => {
    return new Http(backend, defaultOptions);
  }
};

describe('Test MapService', () => {

  beforeEachProviders(() => {
      return [
        MockBackend,
        BaseRequestOptions,
        provide(Http, mockHttpProvider),
        MapService
      ];
    });

    it(' should cache home marker', async(inject([MapService], (testService: MapService) => {
        let setItemSpy = spyOn(localStorage, 'setItem');
        testService.cacheHome(generateFakeHome());
        expect(setItemSpy).toHaveBeenCalled();
        expect(setItemSpy.calls.first().args[0]).toEqual('home');
        expect(JSON.parse(setItemSpy.calls.first().args[1])).toEqual({
          name: 'home',
          address: 'address',
          location: {
            lat: 15,
            lng: 15
          },
          color: '555555'
        });
    })));

    it(' should return cached marker if exists', async(inject([MapService], (testService: MapService) => {
        let getItemSpy = spyOn(localStorage, 'getItem').and.callFake((key) => {
          return JSON.stringify(generateFakeHome());
        });
        let marker = testService.getCachedHome();
        expect(getItemSpy).toHaveBeenCalledWith('home');
        expect(marker.name).toEqual('home');
        expect(marker.address).toEqual('address');
        expect(marker.location).toBeTruthy();
        expect(marker.location.lat).toEqual(15);
        expect(marker.location.lng).toEqual(15);
        expect(marker.color).toEqual('555555');
    })));

    it(' should return undefined if no cached home exists', async(inject([MapService], (testService: MapService) => {
        let getItemSpy = spyOn(localStorage, 'getItem').and.callFake((key) => {
          return undefined;
        });
        let marker = testService.getCachedHome();
        expect(getItemSpy).toHaveBeenCalledWith('home');
        expect(marker).toBeUndefined();
    })));

    it(' should return undefined if cached value cannot be parsed', async(inject([MapService], (testService: MapService) => {
        let getItemSpy = spyOn(localStorage, 'getItem').and.callFake((key) => {
          return 'bla';
        });
        let marker = testService.getCachedHome();
        expect(getItemSpy).toHaveBeenCalledWith('home');
        expect(marker).toBeUndefined();
    })));

    it(' should extract location from GMaps response', async(inject([MapService, MockBackend], (testService: MapService, backend: MockBackend) => {
      backend.connections.subscribe((connection: MockConnection) => {
        expect(connection.request.method).toBe(RequestMethod.Get);
        expect(connection.request.url.indexOf('Szentendrei%20ut%20135')).toBeGreaterThan(0);
        let response = new ResponseOptions({body: generateMockGeoResponse()});
        connection.mockRespond(new Response(response));
      });
        let locationObs = testService.getLocation('Budapest, Szentendrei ut 135');
        expect(locationObs).toBeTruthy();
        locationObs.subscribe(data => {
          expect(data).toBeTruthy();
          expect(data.lat).toEqual(47.562754);
          expect(data.lng).toEqual(19.048568);
        });
    })));

    it(' should return undefined as location if address is unknown', async(inject([MapService, MockBackend], (testService: MapService, backend: MockBackend) => {
      backend.connections.subscribe((connection: MockConnection) => {
        expect(connection.request.method).toBe(RequestMethod.Get);
        expect(connection.request.url.indexOf('Pointe%20Source%20d\'Argent')).toBeGreaterThan(0);
        let response = new ResponseOptions({body: {
           "results" : [],
           "status" : "ZERO_RESULTS"
        }});
        connection.mockRespond(new Response(response));
      });
        let locationObs = testService.getLocation('Pointe Source d\'Argent');
        expect(locationObs).toBeTruthy();
        locationObs.subscribe(data => {
          expect(data).toBeUndefined();
        });
    })));

    it(' should calculate distance correctly', async(inject([MapService], (testService: MapService) => {
        let distance = testService.calculateDistance({
          lat: 47.562754,
          lng: 19.048568
        }, {
          lat: -4.3370025,
          lng: 55.842307
        });
        expect(distance).toBeGreaterThan(6795499);
        expect(distance).toBeLessThan(6796500);
    })));

});

function generateFakeHome(): Marker {
  return new Marker('home', 'address', new Location(15,15), '555555');
}

function generateMockGeoResponse(): string {
  return `{
     "results" : [
        {
           "address_components" : [
              {
                 "long_name" : "135",
                 "short_name" : "135",
                 "types" : [ "street_number" ]
              },
              {
                 "long_name" : "Szentendrei út",
                 "short_name" : "Szentendrei út",
                 "types" : [ "route" ]
              },
              {
                 "long_name" : "III. kerület",
                 "short_name" : "III. kerület",
                 "types" : [ "political", "sublocality", "sublocality_level_1" ]
              },
              {
                 "long_name" : "Budapest",
                 "short_name" : "Budapest",
                 "types" : [ "locality", "political" ]
              },
              {
                 "long_name" : "Budapest",
                 "short_name" : "Budapest",
                 "types" : [ "administrative_area_level_2", "political" ]
              },
              {
                 "long_name" : "Budapest",
                 "short_name" : "Budapest",
                 "types" : [ "administrative_area_level_1", "political" ]
              },
              {
                 "long_name" : "Magyarország",
                 "short_name" : "HU",
                 "types" : [ "country", "political" ]
              },
              {
                 "long_name" : "1031",
                 "short_name" : "1031",
                 "types" : [ "postal_code" ]
              }
           ],
           "formatted_address" : "Budapest, Szentendrei út 135, 1031 Magyarország",
           "geometry" : {
              "location" : {
                 "lat" : 47.562754,
                 "lng" : 19.048568
              },
              "location_type" : "ROOFTOP",
              "viewport" : {
                 "northeast" : {
                    "lat" : 47.56410298029149,
                    "lng" : 19.0499169802915
                 },
                 "southwest" : {
                    "lat" : 47.5614050197085,
                    "lng" : 19.0472190197085
                 }
              }
           },
           "place_id" : "ChIJPcO4G53ZQUcRdn6ZDw_WnuA",
           "types" : [ "street_address" ]
        }
     ],
     "status" : "OK"
  }`;
}