import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PreferenciasGamificadaComponent } from './preferencias-gamificada.component';

describe('PreferenciasGamificadaComponent', () => {
  let component: PreferenciasGamificadaComponent;
  let fixture: ComponentFixture<PreferenciasGamificadaComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PreferenciasGamificadaComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(PreferenciasGamificadaComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});