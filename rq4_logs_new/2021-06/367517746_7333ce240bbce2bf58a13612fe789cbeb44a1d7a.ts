import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ListarConductorVehiculosComponent } from './listar-conductor-vehiculos.component';

describe('ListarConductorVehiculosComponent', () => {
  let component: ListarConductorVehiculosComponent;
  let fixture: ComponentFixture<ListarConductorVehiculosComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ListarConductorVehiculosComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ListarConductorVehiculosComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});