import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DeleteReportedPostDialogComponent } from './delete-reported-post-dialog.component';

describe('DeleteReportedPostDialogComponent', () => {
  let component: DeleteReportedPostDialogComponent;
  let fixture: ComponentFixture<DeleteReportedPostDialogComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DeleteReportedPostDialogComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DeleteReportedPostDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});