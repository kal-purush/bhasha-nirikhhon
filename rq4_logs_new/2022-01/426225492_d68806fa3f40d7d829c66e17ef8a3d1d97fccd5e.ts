import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ModalEjemploComponent } from './modal-ejemplo.component';

describe('ModalEjemploComponent', () => {
  let component: ModalEjemploComponent;
  let fixture: ComponentFixture<ModalEjemploComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ModalEjemploComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ModalEjemploComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});