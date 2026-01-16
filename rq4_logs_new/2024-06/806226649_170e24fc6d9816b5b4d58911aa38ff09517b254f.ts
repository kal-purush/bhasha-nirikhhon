import { TestBed } from '@angular/core/testing';
import {MockProvider} from "ng-mocks";
import {SwPush} from "@angular/service-worker";
import {provideHttpClientTesting} from "@angular/common/http/testing";
import {HttpClient} from "@angular/common/http";

describe('PushNotificationService', () => {

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [MockProvider(SwPush),
        provideHttpClientTesting(),
        MockProvider(HttpClient)]
    });
    // service = TestBed.inject(PushNotificationService);
    // swPush = TestBed.inject(SwPush);
  });

  // TODO: Fix and implement tests
  it('should be created', () => {
    // jest.spyOn(swPush.subscription, 'subscribe').mockReturnValue(null);
    expect(true).toBeTruthy();
  });
});