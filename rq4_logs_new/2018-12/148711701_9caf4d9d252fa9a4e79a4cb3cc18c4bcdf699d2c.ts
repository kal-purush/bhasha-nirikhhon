import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PushCarouselComponent } from './push-carousel.component';

describe('PushCarouselComponent', () => {
  let component: PushCarouselComponent;
  let fixture: ComponentFixture<PushCarouselComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PushCarouselComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PushCarouselComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});