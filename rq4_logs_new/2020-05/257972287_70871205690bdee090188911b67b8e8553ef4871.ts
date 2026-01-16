import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CheckedReportedPostDialogComponent } from './checked-reported-post-dialog.component';

describe('CheckedReportedPostDialogComponent', () => {
  let component: CheckedReportedPostDialogComponent;
  let fixture: ComponentFixture<CheckedReportedPostDialogComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CheckedReportedPostDialogComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CheckedReportedPostDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});