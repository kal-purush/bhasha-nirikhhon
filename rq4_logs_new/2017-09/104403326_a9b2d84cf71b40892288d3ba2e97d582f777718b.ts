import { TestBed, inject } from '@angular/core/testing';

import { ElectronHwidService } from './electron-hwid.service';

describe('ElectronHwidService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [ElectronHwidService]
    });
  });

  it('should be created', inject([ElectronHwidService], (service: ElectronHwidService) => {
    expect(service).toBeTruthy();
  }));
});