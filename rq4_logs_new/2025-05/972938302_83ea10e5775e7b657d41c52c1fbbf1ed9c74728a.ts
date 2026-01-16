import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NovaAvaliacaoComponent } from './nova-avaliacao.component';

describe('NovaAvaliacaoComponent', () => {
  let component: NovaAvaliacaoComponent;
  let fixture: ComponentFixture<NovaAvaliacaoComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NovaAvaliacaoComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(NovaAvaliacaoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});