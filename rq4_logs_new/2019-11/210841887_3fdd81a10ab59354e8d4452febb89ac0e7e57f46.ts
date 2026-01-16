import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ReportIssueDialogComponent } from './report-issue-dialog.component';

describe('ReportIssueDialogComponent', () => {
  let component: ReportIssueDialogComponent;
  let fixture: ComponentFixture<ReportIssueDialogComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ReportIssueDialogComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ReportIssueDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});