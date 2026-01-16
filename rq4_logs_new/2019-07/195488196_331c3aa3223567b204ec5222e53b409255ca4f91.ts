import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MapFieldsComponent } from './map-fields.component';

describe('MapFieldsComponent', () => {
  let component: MapFieldsComponent;
  let fixture: ComponentFixture<MapFieldsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MapFieldsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MapFieldsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});