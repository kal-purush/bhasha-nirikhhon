import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { EmAnnotationSummaryComponent } from './em-annotation-summary.component';

describe('EmAnnotationSummaryComponent', () => {
  let component: EmAnnotationSummaryComponent;
  let fixture: ComponentFixture<EmAnnotationSummaryComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ EmAnnotationSummaryComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EmAnnotationSummaryComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});