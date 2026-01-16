import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BtnOrcamentoIniciarComponent } from './btn-orcamento-iniciar.component';

describe('BtnOrcamentoIniciarComponent', () => {
  let component: BtnOrcamentoIniciarComponent;
  let fixture: ComponentFixture<BtnOrcamentoIniciarComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BtnOrcamentoIniciarComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BtnOrcamentoIniciarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});