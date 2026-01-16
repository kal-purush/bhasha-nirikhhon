import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { of as observableOf, Subject } from 'rxjs';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import {
  MatCardModule,
  MatPaginatorModule,
  MatIconModule,
  MatFormFieldModule,
  MatInputModule
} from '@angular/material';
import { ActivatedRoute, Router } from '@angular/router';

import { DataReviewComponent } from './data-review.component';
import { ReviewNoteService } from '../../services/review-note.service';
import { ReviewNoteResponse } from 'src/app/interfaces/review-note';
import { PaginatedResponse } from 'src/app/interfaces/paginated-response';
import { RecordResponse } from 'src/app/interfaces/record';

describe('DataReviewComponent', () => {
  let component: DataReviewComponent;
  let fixture: ComponentFixture<DataReviewComponent>;
  let getReviewNotes$: Subject<PaginatedResponse<ReviewNoteResponse>>;
  const dummyReviewNote: ReviewNoteResponse = {
    id: 1,
    create_time: '2019-02-23T20:37:57.127073+08:00',
    field_name: 'time',
    content: '组织自己电子国内控制一次登录这样能够',
    record: 2,
    user: 48,
  };

  beforeEach(async(() => {
    getReviewNotes$ = new Subject<PaginatedResponse<ReviewNoteResponse>>();
    TestBed.configureTestingModule({
      declarations: [ DataReviewComponent ],
      imports: [
        MatCardModule,
        MatPaginatorModule,
        MatIconModule,
        MatFormFieldModule,
        MatInputModule,
        NoopAnimationsModule
      ],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            data: observableOf({record: {
              id: 1,
              create_time: '2019-01-01',
              update_time: '2019-01-02',
              campus_event: null,
              off_campus_event: {id: 1,
                                 create_time: '2019-03-02T09:07:57.159755+08:00',
                                 update_time: '2019-03-02T09:07:57.159921+08:00',
                                 name: 'sfdg',
                                 time: '2019-03-02T00:00:00+08:00',
                                 location: 'dfgfd',
                                 num_hours: 0,
                                 num_participants: 25
                                 },
              contents: [],
              attachments: [],
              user: 1,
              status: 1,
            } as RecordResponse}),
          }
        },
        {
          provide: Router,
          useValue: {}
        },
        {
          provide: ReviewNoteService,
          useValue: {
            getReviewNotes: () => getReviewNotes$,
          }
        },
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DataReviewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should load data', () => {
    const results: ReviewNoteResponse[] = [dummyReviewNote, dummyReviewNote];
    getReviewNotes$.next({
        count: 100,
        results,
        next: '',
        previous: ''
    });

    expect(component.isLoadingResults).toBeFalsy();
    expect(component.results).toEqual(results);
    expect(component.resultsLength).toEqual(100);
  });

  it('should empty data if an error encountered.', () => {
    getReviewNotes$.error('a');

    expect(component.isLoadingResults).toBeFalsy();
    expect(component.results).toEqual([]);
    expect(component.resultsLength).toEqual(0);
  });

});