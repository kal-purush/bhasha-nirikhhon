import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ExibirEventoComponent } from './exibir-evento.component';

describe('ExibirEventoComponent', () => {
  let component: ExibirEventoComponent;
  let fixture: ComponentFixture<ExibirEventoComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ExibirEventoComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ExibirEventoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});