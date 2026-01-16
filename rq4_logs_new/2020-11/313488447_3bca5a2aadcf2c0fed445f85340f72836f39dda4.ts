import { ComponentFixture, TestBed } from '@angular/core/testing';

import { HeroColorComponent } from './hero-color.component';

describe('HeroColorComponent', () => {
  let component: HeroColorComponent;
  let fixture: ComponentFixture<HeroColorComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ HeroColorComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HeroColorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});