import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { FoodDescComponent } from './food-desc.component';

describe('FoodDescComponent', () => {
  let component: FoodDescComponent;
  let fixture: ComponentFixture<FoodDescComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ FoodDescComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(FoodDescComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});