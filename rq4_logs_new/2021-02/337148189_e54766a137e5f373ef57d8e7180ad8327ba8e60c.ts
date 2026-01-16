import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RevisaoCadastroProdutosComponent } from './revisao-cadastro-produtos.component';

describe('RevisaoCadastroProdutosComponent', () => {
  let component: RevisaoCadastroProdutosComponent;
  let fixture: ComponentFixture<RevisaoCadastroProdutosComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ RevisaoCadastroProdutosComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RevisaoCadastroProdutosComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});