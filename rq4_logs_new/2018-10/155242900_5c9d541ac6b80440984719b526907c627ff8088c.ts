import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MapEntryComponent } from './map-entry.component';

describe('MapEntryComponent', () => {
  let component: MapEntryComponent;
  let fixture: ComponentFixture<MapEntryComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MapEntryComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MapEntryComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});