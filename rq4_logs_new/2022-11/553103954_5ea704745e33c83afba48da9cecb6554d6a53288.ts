import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ManejadorUsuarioComponent } from './manejador-usuario.component';

describe('ManejadorUsuarioComponent', () => {
  let component: ManejadorUsuarioComponent;
  let fixture: ComponentFixture<ManejadorUsuarioComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ManejadorUsuarioComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ManejadorUsuarioComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});