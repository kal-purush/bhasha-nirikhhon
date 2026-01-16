import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ImagineAdminComponent } from './imagine-admin.component';

describe('ImagineAdminComponent', () => {
  let component: ImagineAdminComponent;
  let fixture: ComponentFixture<ImagineAdminComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ImagineAdminComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ImagineAdminComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});