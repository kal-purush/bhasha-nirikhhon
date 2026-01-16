import {
  it,
  inject,
  injectAsync,
  beforeEachProviders,
  TestComponentBuilder
} from 'angular2/testing';

import {Component, provide} from 'angular2/core';
import {BaseRequestOptions, XHRBackend, ResponseOptions, Response, Http} from 'angular2/http';
import {MockBackend, MockConnection} from 'angular2/http/testing';

import {MyHttp} from './my-http.service';

describe('Title', () => {
  beforeEachProviders(() => [
    BaseRequestOptions,
    MockBackend,
    provide(Http, {
      useFactory: function(backend, defaultOptions) {
        return new Http(backend, defaultOptions);
      },
      deps: [MockBackend, BaseRequestOptions]
    }),

    MyHttp
  ]);


  it('should return status', inject([ MyHttp ], (myhttp) => {
    expect(!!myhttp.getStatus()).toEqual(true);
  }));

  it('should get data from the server even if connection is down at first', injectAsync([ XHRBackend, MyHttp ], (mockBackend, myhttp) => {
    // mock online status
    var status = false;

    mockBackend.connections.subscribe(
      (connection: MockConnection) => {
        if (status) {
          connection.mockRespond(new Response(
            new ResponseOptions({
                body: [
                  {
                    id: 26,
                    contentRendered: "<p><b>Hi there</b></p>",
                    contentMarkdown: "*Hi there*"
                  }]
              }
            )));
        } else {
          connection.mockError(new Error('404'));
        }
      });

    var counter = 0;

    myhttp.get('/').map(res=>res.json()).subscribe(res => {
      switch (counter++) {
        case 0:
              expect(res.length).toBe(0);
              // set connection to "online"
              status = true;
              break;
        case 1:
              expect(res.length).toBe(1);
              break;

      }
    });



    /*spyOn(console, 'log');
    expect(console.log).not.toHaveBeenCalled();

    myhttp.get('').getData();
    expect(console.log).toHaveBeenCalled();
    expect(title.getData()).toEqual({ value: 'AngularClass' });*/
  }));

});