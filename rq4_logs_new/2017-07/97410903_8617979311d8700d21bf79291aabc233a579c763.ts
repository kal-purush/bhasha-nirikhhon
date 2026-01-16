import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { JobeditComponent } from './jobedit.component';

describe('JobeditComponent', () => {
  let component: JobeditComponent;
  let fixture: ComponentFixture<JobeditComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ JobeditComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(JobeditComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});