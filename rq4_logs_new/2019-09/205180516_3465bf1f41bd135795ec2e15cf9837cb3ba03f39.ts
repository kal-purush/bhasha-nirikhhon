import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MapLayerComponent } from './map-layer.component';

describe('MapLayerComponent', () => {
  let component: MapLayerComponent;
  let fixture: ComponentFixture<MapLayerComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [MapLayerComponent],
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MapLayerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});