import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AceptarServicioComponent } from './aceptar-servicio.component';

describe('AceptarServicioComponent', () => {
  let component: AceptarServicioComponent;
  let fixture: ComponentFixture<AceptarServicioComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ AceptarServicioComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AceptarServicioComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});