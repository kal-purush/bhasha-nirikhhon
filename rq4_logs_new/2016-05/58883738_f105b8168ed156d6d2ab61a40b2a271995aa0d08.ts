/*
// import {beforeEachProviders, it, describe, expect, inject} from '@angular/core/testing';
// import {NlpService} from './nlp.service';
// import {Http, BaseRequestOptions} from '@angular/http';
// import {MockBackend, Response} from '@angular/http/testing';
// import {Injector} from '@angular/core';
//
// describe('Nlp Service', () => {
//   beforeEachProviders(() => [NlpService, Http]);

// let aRecipe = `1 cup chopped seeded Roma tomatoes
//     1 cup jarred roasted red 
//     1/4 cup low-sodium chicken broth
//     2 tsp olive oil
//     1 clove garlic, minced
//     1 tbsp fresh lemon juice`;
//
// it('should get a response', () => {
//   var connection;  // this will be set when a new connection is emitted from the backend.
//   var text;        // this will be set from mock response
//
//   var injector = Injector.resolveAndCreate([
//     MockBackend,
//     provide(Http, {
//       useFactory: (backend, options) => {
//     return new Http(backend, options);
//       }, deps: [MockBackend, BaseRequestOptions]
//     }]);
//
//   var backend = injector.get(MockBackend);
//   var http = injector.get(Http);
//   backend.connections.subscribe(c => connection = c);
//   http.request('something.json').subscribe(res => {
//     text = res.text();
//   });
//   // connection.mockRespond(new Response({body: 'Something'}));
//   expect(text).toBe('Something');
// });

// it('should ...', inject(
//   [NlpService, Http, ConnectionBackend, RequestOptions],
//   (service:NlpService) => {
//     expect(service).toBeTruthy();
//   }));
//
// it('should find potatoe from the recipe',
//   inject(
//     [NlpService, Http, ConnectionBackend, RequestOptions],
//     (service:NlpService) => {
//       expect(service.getIsles(aRecipe)).toEqual(['potatoes']);
//     }));
// });
*/