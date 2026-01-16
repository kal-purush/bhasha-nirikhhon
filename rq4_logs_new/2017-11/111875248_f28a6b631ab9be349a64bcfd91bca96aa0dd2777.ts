import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ProyectosCrudComponent } from './proyectos-crud.component';

describe('ProyectosCrudComponent', () => {
  let component: ProyectosCrudComponent;
  let fixture: ComponentFixture<ProyectosCrudComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ProyectosCrudComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ProyectosCrudComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});