import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ComputadoraDetalleComponent } from './computadora-detalle.component';

describe('ComputadoraDetalleComponent', () => {
  let component: ComputadoraDetalleComponent;
  let fixture: ComponentFixture<ComputadoraDetalleComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ComputadoraDetalleComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ComputadoraDetalleComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});