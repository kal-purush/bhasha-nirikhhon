import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { of as observableOf } from 'rxjs';
import { MatCardModule, MatIconModule } from '@angular/material';
import { ActivatedRoute } from '@angular/router';
import { Location } from '@angular/common';

import { DataReviewComponent } from './data-review.component';
import { RecordResponse } from 'src/app/interfaces/record';

describe('DataReviewComponent', () => {
  let component: DataReviewComponent;
  let fixture: ComponentFixture<DataReviewComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DataReviewComponent ],
      imports: [
        MatCardModule,
        MatIconModule,
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
          },
        },
        {
          provide: Location
        }
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
});