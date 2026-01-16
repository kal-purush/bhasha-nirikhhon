import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DvirDefectCheckFormComponent } from './dvir-defect-check-form.component';

describe('DvirDefectCheckFormComponent', () => {
  let component: DvirDefectCheckFormComponent;
  let fixture: ComponentFixture<DvirDefectCheckFormComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DvirDefectCheckFormComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DvirDefectCheckFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});