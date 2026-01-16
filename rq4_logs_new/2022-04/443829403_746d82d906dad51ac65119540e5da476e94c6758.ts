import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { OwnerRestaurantsComponent } from './owner-restaurants.component';

describe('OwnerRestaurantsComponent', () => {
  let component: OwnerRestaurantsComponent;
  let fixture: ComponentFixture<OwnerRestaurantsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ OwnerRestaurantsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OwnerRestaurantsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});