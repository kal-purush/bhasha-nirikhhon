import { TestBed } from '@angular/core/testing';

import { WebSocketNotificationService } from './web-socket-notification.service';

describe('WebSocketNotificationService', () => {
  let service: WebSocketNotificationService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(WebSocketNotificationService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});