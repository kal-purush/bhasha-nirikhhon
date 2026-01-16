import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { JobaddComponent } from './jobadd.component';

describe('JobaddComponent', () => {
  let component: JobaddComponent;
  let fixture: ComponentFixture<JobaddComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ JobaddComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(JobaddComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});