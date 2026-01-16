import { TestBed, inject } from '@angular/core/testing';

import { StockService } from './stock.service';

describe('StockService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [StockService]
    });
  });

  it('should be created', inject([StockService], (service: StockService) => {
    expect(service).toBeTruthy();
  }));

  it('should return stock list', inject([StockService], (service: StockService) => {
    const stock = service.getStocks();
    expect(stock).not.toBeNull();
  }));

});