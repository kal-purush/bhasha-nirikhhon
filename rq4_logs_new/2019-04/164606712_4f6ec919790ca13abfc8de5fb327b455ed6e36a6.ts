import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';

import { ReviewNoteService } from './review-note.service';

import { environment } from 'src/environments/environment';

describe('ReviewNoteService', () => {
  let httpTestingController: HttpTestingController;
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [
        HttpClientTestingModule,
      ],
    });
    httpTestingController = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTestingController.verify();
  });

  it('should be created', () => {
    const service: ReviewNoteService = TestBed.get(ReviewNoteService);
    expect(service).toBeTruthy();
  });

  it('should get reviewnotes', () => {
    const service: ReviewNoteService = TestBed.get(ReviewNoteService);
    const url = 'review-notes';
    const extraParams = new Map<string, {}>([['record', 3], ['user', 87]]);

    service.getReviewNotes({ extraParams }).subscribe();

    const expectedUrl = `${environment.API_URL}/${url}/?limit=${environment.PAGINATION_SIZE}&offset=0&record=3&user=87`;

    const req = httpTestingController.expectOne(expectedUrl);
    console.log(url);
    expect(req.request.method).toEqual('GET');
    req.flush({});
  });
});