import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ServiceAdminComponent } from './service-admin.component';

describe('ServiceAdminComponent', () => {
  let component: ServiceAdminComponent;
  let fixture: ComponentFixture<ServiceAdminComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ServiceAdminComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(ServiceAdminComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});