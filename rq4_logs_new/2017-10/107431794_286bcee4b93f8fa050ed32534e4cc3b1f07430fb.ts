import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SubmissionPageComponent } from './submission-page.component';

describe('SubmissionPageComponent', () => {
  let component: SubmissionPageComponent;
  let fixture: ComponentFixture<SubmissionPageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SubmissionPageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SubmissionPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});