import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AddEmployeeToCourseComponent } from './add-employee-to-course.component';

describe('AddEmployeeToCourseComponent', () => {
  let component: AddEmployeeToCourseComponent;
  let fixture: ComponentFixture<AddEmployeeToCourseComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AddEmployeeToCourseComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AddEmployeeToCourseComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});