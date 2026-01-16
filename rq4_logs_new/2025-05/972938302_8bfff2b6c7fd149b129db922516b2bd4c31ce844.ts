import { ComponentFixture, TestBed } from '@angular/core/testing';

import { VisualizarAvaliacoesComponent } from './visualizar-avaliacoes.component';

describe('VisualizarAvaliacoesComponent', () => {
  let component: VisualizarAvaliacoesComponent;
  let fixture: ComponentFixture<VisualizarAvaliacoesComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [VisualizarAvaliacoesComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(VisualizarAvaliacoesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});