//Originall taken from https://github.com/mhadaily/ng2-barcode-validator/blob/master/src/app/services/barcode-decoder.service.spec.ts
/* tslint:disable:no-unused-variable */

import { TestBed, async, inject } from '@angular/core/testing';
import { BarcodeDecoderService } from './barcode-decoder.service';

describe('BarcodeDecoderService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [BarcodeDecoderService]
    });
  });

  it('should ...', inject([BarcodeDecoderService], (service: BarcodeDecoderService) => {
    expect(service).toBeTruthy();
  }));
});