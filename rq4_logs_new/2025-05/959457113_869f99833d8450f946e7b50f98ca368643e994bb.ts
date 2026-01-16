import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ProyectoModalComponent } from './proyecto-modal.component';

describe('Proyecto1ModalComponent', () => {
  let component: ProyectoModalComponent;
  let fixture: ComponentFixture<ProyectoModalComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ProyectoModalComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ProyectoModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});