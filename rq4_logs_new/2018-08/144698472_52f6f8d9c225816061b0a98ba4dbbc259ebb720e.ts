import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AirPolutionComponent } from './air-polution.component';

describe('AirPolutionComponent', () => {
  let component: AirPolutionComponent;
  let fixture: ComponentFixture<AirPolutionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AirPolutionComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AirPolutionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});