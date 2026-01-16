import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { WriteRatingComponent } from './write-rating.component';

describe('WriteRatingComponent', () => {
  let component: WriteRatingComponent;
  let fixture: ComponentFixture<WriteRatingComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ WriteRatingComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(WriteRatingComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});