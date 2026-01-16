import { TestBed, fakeAsync, tick } from '@angular/core/testing';
import { HttpTestingController, HttpClientTestingModule } from '@angular/common/http/testing';

import { JwtHelperService } from "@auth0/angular-jwt";
import { mock, instance, when, anyString, anything } from 'ts-mockito';

import { AuthenticatorService } from './authenticator.service';

describe('AuthenticatorService', () => {
  let service: AuthenticatorService;
  let httpMock: HttpTestingController;

  const mockedStorage = mock(Storage);
  const mockedJwtService = mock(JwtHelperService);

  when(mockedJwtService.decodeToken(anything())).thenReturn({ unique_name: 'xpto', token: 'someToken' });

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [
        HttpClientTestingModule
      ],
      providers: [
        { provide: Storage, useValue: instance(mockedStorage) },
        { provide: JwtHelperService, useValue: instance(mockedJwtService) },
        { provide: 'LOGIN_URL', useValue: 'http://testeUrl' }
      ]
    });

    service = TestBed.inject(AuthenticatorService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('#login', () => {
    describe('when response is 200', () => {
      it('should return loggedIn true', fakeAsync(() => {
        let result: boolean;

        service.login({ user: 'xpto', password: 'xpto' }).subscribe(r => result = r);

        httpMock.expectOne("http://testeUrl").flush({ token: 'xpto' }, { status: 200, statusText: 'Ok' });

        tick();

        expect(result).toBeTrue();
      }));
    });

    describe('when error occur', () => {
      it('should return loggedIn false', fakeAsync(() => {
        let result: boolean;

        service.login({ user: 'xpto', password: 'xpto' }).subscribe(r => result = r);;

        httpMock.expectOne("http://testeUrl").flush({}, { status: 400, statusText: 'Not ok' });

        tick();

        expect(result).toBeFalse();
      }));
    });
  });
});