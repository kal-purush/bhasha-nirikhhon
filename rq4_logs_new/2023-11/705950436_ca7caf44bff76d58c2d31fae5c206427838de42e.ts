import { ComponentFixture, TestBed } from '@angular/core/testing';

import { FoodByCategoryComponent } from './food-by-category.component';

describe('FoodByCategoryComponent', () => {
  let component: FoodByCategoryComponent;
  let fixture: ComponentFixture<FoodByCategoryComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [FoodByCategoryComponent]
    });
    fixture = TestBed.createComponent(FoodByCategoryComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});