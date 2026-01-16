import { TestBed, inject } from '@angular/core/testing';
import { MockBackend } from '@angular/http/testing';
import { MockType, MockResponse } from '../../../helpers/mock-response';
import { Http, Response, BaseRequestOptions } from '@angular/http';

import { DataService } from './data.service';

describe('DataService', () => {
  let dataService: DataService;
  let mockBackend: MockBackend;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        DataService,
        MockBackend,
        BaseRequestOptions,
        {
         provide: Http,
         useFactory: (_mockBackend, _options) => {
           return new Http(_mockBackend, _options);
         },
         deps: [MockBackend, BaseRequestOptions]
       }
      ]
    });
  });

  beforeEach(inject([DataService, MockBackend],(_DataService_, _MockBackend_) => {
    dataService = _DataService_;
    mockBackend = _MockBackend_;
  }));

  it('should ...', () => {
    expect(dataService).toBeTruthy();
  });

  it('should retreive the jobs data - getJobsData()', () => {
   setMockBackend({fake: 'test'}, '../../../assets/jobs.json', 200);

   dataService.getJobsData()
     .subscribe((x) => {
       expect(x).toEqual({fake: 'test'});
     });
 });


  function setMockBackend(body, url, status) {
   return MockResponse(mockBackend, {
       body: JSON.stringify(body),
       status: status
     },
     MockType.Response,
     (connection) => {
       console.log(connection);
       expect(connection.request.url)
         .toEqual(url);
     }
   );
 };
});