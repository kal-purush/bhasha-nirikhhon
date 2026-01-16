import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SymptomManagementFormComponent } from './symptom-management-form.component';

describe('SymptomManagementFormComponent', () => {
  let component: SymptomManagementFormComponent;
  let fixture: ComponentFixture<SymptomManagementFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ SymptomManagementFormComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SymptomManagementFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});