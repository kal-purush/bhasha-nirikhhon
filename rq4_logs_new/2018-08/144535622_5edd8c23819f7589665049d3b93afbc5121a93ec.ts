import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CompanyWorkTypeDropdownComponent } from './company-work-type-dropdown.component';

describe('CompanyWorkTypeDropdownComponent', () => {
  let component: CompanyWorkTypeDropdownComponent;
  let fixture: ComponentFixture<CompanyWorkTypeDropdownComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CompanyWorkTypeDropdownComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CompanyWorkTypeDropdownComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});