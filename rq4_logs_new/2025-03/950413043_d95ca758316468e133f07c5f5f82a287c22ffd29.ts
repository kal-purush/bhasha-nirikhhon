import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ClienteEmpleadoComponent } from './cliente-empleado.component';

describe('ClienteEmpleadoComponent', () => {
  let component: ClienteEmpleadoComponent;
  let fixture: ComponentFixture<ClienteEmpleadoComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ClienteEmpleadoComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ClienteEmpleadoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});