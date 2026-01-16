import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TestimonialsStudentsComponent } from './testimonials-students.component';

describe('TestimonialsStudentsComponent', () => {
  let component: TestimonialsStudentsComponent;
  let fixture: ComponentFixture<TestimonialsStudentsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TestimonialsStudentsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TestimonialsStudentsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});