import { ComponentFixture, TestBed } from '@angular/core/testing';

import { HeroImgComponent } from './hero-img.component';

describe('HeroImgComponent', () => {
  let component: HeroImgComponent;
  let fixture: ComponentFixture<HeroImgComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ HeroImgComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HeroImgComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});