import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CityMapComponent } from './city-map.component';

describe('CityMapComponent', () => {
  let component: CityMapComponent;
  let fixture: ComponentFixture<CityMapComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CityMapComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CityMapComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});