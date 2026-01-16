// import { TestBed } from '@angular/core/testing';
// import {
//   HttpClientTestingModule,
//   HttpTestingController
// } from '@angular/common/http/testing';
//
// import { AuthHeaderInterceptor } from './auth-header.interceptor';
// import { AuthService, LocalStorageService } from '@app/core';
// import {
//   authServiceSpyFactory,
//   localStorageServiceSpyFactory
// } from '@app/core/test-doubles';
// import { ApiRoute, LocalStorageKey } from '@assets/constants';
// import { HTTP_INTERCEPTORS } from '@angular/common/http';
//
// describe('AuthHeaderInterceptor', () => {
//   let interceptor: AuthHeaderInterceptor;
//   let httpTestingController: HttpTestingController;
//   const authToken = 'authToken123456789';
//
//   beforeEach(() => {
//     TestBed.configureTestingModule({
//       imports: [HttpClientTestingModule],
//       providers: [
//         AuthHeaderInterceptor,
//         {
//           provide: HTTP_INTERCEPTORS,
//           useClass: AuthHeaderInterceptor,
//           multi: true
//         },
//         {
//           provide: AuthService,
//           useFactory: authServiceSpyFactory
//         }
//       ]
//     });
//
//     interceptor = TestBed.get(AuthHeaderInterceptor);
//     httpTestingController = TestBed.get(HttpTestingController);
//   });
//
//   it('should be created', () => {});
//
//   it('should add header when request contains api url', () => {
//     const httpRequest = httpTestingController.expectOne(`${ApiRoute.BASE}/xxx`);
//     expect(httpRequest.request.headers['Authorization']).toEqual(
//       `Bearer ${authToken}`
//     );
//   });
//
//   it('should not add header when request does not contain api url', () => {
//     const httpRequest = httpTestingController.expectOne('/xxx');
//     expect(httpRequest.request.headers.has('Authorization')).toBeFalsy();
//   });
// });