import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { HeroStoreComponent } from './hero-store.component';

describe('HeroStoreComponent', () => {
  let component: HeroStoreComponent;
  let fixture: ComponentFixture<HeroStoreComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ HeroStoreComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(HeroStoreComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});