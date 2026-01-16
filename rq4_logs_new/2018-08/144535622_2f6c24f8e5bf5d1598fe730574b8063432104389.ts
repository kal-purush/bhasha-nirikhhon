import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CompanyWorkTypeManageComponent } from './company-work-type-manage.component';

describe('CompanyWorkTypeManageComponent', () => {
  let component: CompanyWorkTypeManageComponent;
  let fixture: ComponentFixture<CompanyWorkTypeManageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CompanyWorkTypeManageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CompanyWorkTypeManageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});