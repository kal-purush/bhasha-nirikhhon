import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { OneWayTogglerComponent } from './one-way-toggler.component';

describe('OneWayTogglerComponent', () => {
  let component: OneWayTogglerComponent;
  let fixture: ComponentFixture<OneWayTogglerComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ OneWayTogglerComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OneWayTogglerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});