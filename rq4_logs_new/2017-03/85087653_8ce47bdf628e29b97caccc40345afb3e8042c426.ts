import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BsCarouselComponent } from './bs-carousel.component';

describe('BsCarouselComponent', () => {
  let component: BsCarouselComponent;
  let fixture: ComponentFixture<BsCarouselComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BsCarouselComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BsCarouselComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});