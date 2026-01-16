import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MapaPageComponent } from './mapa-page.component';

describe('MapaPageComponent', () => {
  let component: MapaPageComponent;
  let fixture: ComponentFixture<MapaPageComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ MapaPageComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(MapaPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});