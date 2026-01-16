import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MapTestComponentComponent } from './map-test-component.component';

describe('MapTestComponentComponent', () => {
  let component: MapTestComponentComponent;
  let fixture: ComponentFixture<MapTestComponentComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ MapTestComponentComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MapTestComponentComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});