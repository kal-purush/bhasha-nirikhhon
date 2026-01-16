import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ManagerServicesComponent } from './manager-services.component';

describe('ManagerServicesComponent', () => {
  let component: ManagerServicesComponent;
  let fixture: ComponentFixture<ManagerServicesComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [ManagerServicesComponent]
    });
    fixture = TestBed.createComponent(ManagerServicesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});