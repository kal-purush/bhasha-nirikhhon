import { ComponentFixture, TestBed } from '@angular/core/testing';

import { GenericCarouselComponent } from './generic-carousel.component';

describe('GenericCarouselComponent', () => {
  let component: GenericCarouselComponent;
  let fixture: ComponentFixture<GenericCarouselComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [GenericCarouselComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(GenericCarouselComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});