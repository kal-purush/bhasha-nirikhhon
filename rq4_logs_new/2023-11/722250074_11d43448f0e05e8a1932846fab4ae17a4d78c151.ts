import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TablaDatosSensorComponent } from './tabla-datos-sensor.component';

describe('TablaDatosSensorComponent', () => {
  let component: TablaDatosSensorComponent;
  let fixture: ComponentFixture<TablaDatosSensorComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ TablaDatosSensorComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(TablaDatosSensorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});