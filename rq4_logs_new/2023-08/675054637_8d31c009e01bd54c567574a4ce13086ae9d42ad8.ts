import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AlliedCompaniesComponent } from './allied-companies.component';

describe('AlliedCompaniesComponent', () => {
  let component: AlliedCompaniesComponent;
  let fixture: ComponentFixture<AlliedCompaniesComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ AlliedCompaniesComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(AlliedCompaniesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});