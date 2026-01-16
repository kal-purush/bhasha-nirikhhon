import { ComponentFixture, TestBed } from '@angular/core/testing';

import { InsumoInfoComponent } from './insumo-info.component';

describe('InsumoInfoComponent', () => {
  let component: InsumoInfoComponent;
  let fixture: ComponentFixture<InsumoInfoComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ InsumoInfoComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(InsumoInfoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});