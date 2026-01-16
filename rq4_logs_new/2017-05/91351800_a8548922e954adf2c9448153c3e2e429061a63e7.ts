import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SwapiPlanetsComponent } from './swapi-planets.component';

describe('SwapiPlanetsComponent', () => {
  let component: SwapiPlanetsComponent;
  let fixture: ComponentFixture<SwapiPlanetsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SwapiPlanetsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SwapiPlanetsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});