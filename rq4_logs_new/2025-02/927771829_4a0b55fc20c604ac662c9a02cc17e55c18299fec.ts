import { TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting, HttpTestingController } from '@angular/common/http/testing';
import { ThinkingService } from './thinking.service';
import { Thinking } from '../interfaces/thinking';
import { environment } from '../../environments/environment';

describe('ThinkingService', () => {
  let service: ThinkingService;
  let httpMock: HttpTestingController;
  const API = environment.apiUrl

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        ThinkingService,
        provideHttpClient(),
        provideHttpClientTesting(),
      ],
    })
    service = TestBed.inject(ThinkingService);
    httpMock = TestBed.inject(HttpTestingController);
  })
  afterEach(() => {
    httpMock.verify();
  })
  it('should retrieve a list of thinkings', () => {
    const mockThinkings: Thinking[] = [
      {
        id: 1, content: 'Test 1',
        auth: '',
        model: ''
      },
      {
        id: 2, content: 'Test 2',
        auth: '',
        model: ''
      },
    ];

    service.list().subscribe((thinkings) => {
      expect(thinkings.length).toBe(2);
      expect(thinkings).toEqual(mockThinkings);
    });

    const req = httpMock.expectOne(`${API}/thinkings`);
    expect(req.request.method).toBe('GET');
    req.flush(mockThinkings);
  });

})