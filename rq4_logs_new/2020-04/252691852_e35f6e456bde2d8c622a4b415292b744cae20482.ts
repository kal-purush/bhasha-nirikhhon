import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { HorserowComponent } from './horserow.component';

describe('HorserowComponent', () => {
  let component: HorserowComponent;
  let fixture: ComponentFixture<HorserowComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ HorserowComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(HorserowComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});