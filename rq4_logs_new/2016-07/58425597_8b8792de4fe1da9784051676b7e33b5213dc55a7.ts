import {
  beforeEachProviders,
  it,
  describe,
  expect,
  inject
} from '@angular/core/testing';
import { provide} from '@angular/core';
import { Headers, HTTP_PROVIDERS, BaseRequestOptions, XHRBackend, Response } from '@angular/http';
import { MockBackend, MockConnection } from '@angular/http/testing';
import { AUTH_PROVIDERS, authService } from 'angular2-devise-token-auth';
import { AuthenticationService } from './authentication.service';

describe('AuthenticationService', () => {
  beforeEachProviders(() => {
    return [
      provide(XHRBackend, {useClass: MockBackend}),
      AUTH_PROVIDERS,
      authService(`http://localhost/auth`),
      AuthenticationService
    ];
  });

  it('should inject the service', inject([AuthenticationService], (service: AuthenticationService) => {
    expect(service).toBeTruthy();
  }));

  it('should sign in a user', inject([AuthenticationService], (service: AuthenticationService) => {

  }));
});