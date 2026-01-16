import { TestBed, inject } from '@angular/core/testing';
import { Http, BaseRequestOptions, Response, ResponseOptions, RequestMethod } from '@angular/http';
import { MockBackend, MockConnection } from '@angular/http/testing';
import { GoodsService } from './goods';

fdescribe('Service: Goods', () => {
  let service: GoodsService = null;
  let backend: MockBackend = null;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        MockBackend,
        BaseRequestOptions,
        {
          provide: Http,
          useFactory: (backendInstance: MockBackend, defaultOptions: BaseRequestOptions) => {
            return new Http(backendInstance, defaultOptions);
          },
          deps: [ MockBackend, BaseRequestOptions ]
        },
        GoodsService
      ]
    });
  });

  beforeEach(inject([GoodsService, MockBackend], (goodsService: GoodsService, mockBackend: MockBackend) => {
    service = goodsService;
    backend = mockBackend;
  }));

  it('should call the get list api and return goods list', (done) => {
    const mockData = [
      {
        id: 10,
        name: 'book #10 name'
      },
      {
        id: 11,
        name: 'book #11 name'
      },
      {
        id: 12,
        name: 'book #12 name'
      }
    ];
    const mockedResponse = {
      items: mockData
    };

    backend.connections.subscribe((connection: MockConnection) => {
      const options = new ResponseOptions({
        body: JSON.stringify(mockedResponse)
      });
      connection.mockRespond(new Response(options));
      expect(connection.request.method).toEqual(RequestMethod.Get);
      expect(connection.request.url).toEqual(`/api/goods`);
    });

    service
      .getAllGoods()
      .subscribe((res) => {
        expect(res).toEqual(mockData);
        done();
      });
  });

  it('should call the get api and return the good info', (done) => {
    const mockData = {
      id: 10,
      name: 'book #10 name'
    };

    backend.connections.subscribe((connection: MockConnection) => {
      const options = new ResponseOptions({
        body: JSON.stringify(mockData)
      });
      connection.mockRespond(new Response(options));
      expect(connection.request.method).toEqual(RequestMethod.Get);
      expect(connection.request.url).toEqual(`/api/goods/10`);
    });

    service
      .getGood(10)
      .subscribe((res) => {
        expect(res).toEqual(mockData);
        done();
      });
  });

  it('should call the update api and return nothing', (done) => {
    const mockUpdateData = {
      id: 10,
      name: 'book #10 name'
    };

    backend.connections.subscribe((connection: MockConnection) => {
      const options = new ResponseOptions({
        body: JSON.stringify('')
      });
      connection.mockRespond(new Response(options));
      expect(connection.request.method).toEqual(RequestMethod.Put);
      expect(JSON.parse(connection.request.getBody())).toEqual(mockUpdateData);
      expect(connection.request.url).toEqual(`/api/goods/10`);
    });

    service
      .updateGood(10, mockUpdateData)
      .subscribe((res) => {
        expect(res).toEqual('');
        done();
      });
  });
});