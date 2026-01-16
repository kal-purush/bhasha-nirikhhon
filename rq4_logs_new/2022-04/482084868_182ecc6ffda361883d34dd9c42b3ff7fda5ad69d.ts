import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ModalMudaCidadeComponent } from './modal-muda-cidade.component';

describe('ModalMudaCidadeComponent', () => {
  let component: ModalMudaCidadeComponent;
  let fixture: ComponentFixture<ModalMudaCidadeComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ModalMudaCidadeComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ModalMudaCidadeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});