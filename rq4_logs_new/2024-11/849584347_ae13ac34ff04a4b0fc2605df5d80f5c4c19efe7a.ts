import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AgregarServicioAdminComponent } from './Agregar-Servicio-Admin/agregar-servicio-admin.component';

describe('AgregarServicioAdminComponent', () => {
  let component: AgregarServicioAdminComponent;
  let fixture: ComponentFixture<AgregarServicioAdminComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [AgregarServicioAdminComponent]
    });
    fixture = TestBed.createComponent(AgregarServicioAdminComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});