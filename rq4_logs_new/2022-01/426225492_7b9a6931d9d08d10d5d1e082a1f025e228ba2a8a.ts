import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RutaUsuarioPerfilComponent } from './ruta-usuario-perfil.component';

describe('RutaUsuarioPerfilComponent', () => {
  let component: RutaUsuarioPerfilComponent;
  let fixture: ComponentFixture<RutaUsuarioPerfilComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ RutaUsuarioPerfilComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RutaUsuarioPerfilComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});