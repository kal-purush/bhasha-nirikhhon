import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NavbarInicioEmpresasComponent } from './navbar-inicio-empresas.component';

describe('NavbarInicioEmpresasComponent', () => {
  let component: NavbarInicioEmpresasComponent;
  let fixture: ComponentFixture<NavbarInicioEmpresasComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NavbarInicioEmpresasComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NavbarInicioEmpresasComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});