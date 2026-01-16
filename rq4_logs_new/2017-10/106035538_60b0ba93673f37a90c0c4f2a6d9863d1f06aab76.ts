import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { HikeCardSliderComponent } from './hike-card-slider.component';

describe('HikeCardSliderComponent', () => {
  let component: HikeCardSliderComponent;
  let fixture: ComponentFixture<HikeCardSliderComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ HikeCardSliderComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(HikeCardSliderComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});