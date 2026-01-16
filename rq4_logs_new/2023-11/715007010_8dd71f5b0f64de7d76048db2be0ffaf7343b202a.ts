import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MyRecipeComponent } from './my-recipe.component';

describe('MyRecipeComponent', () => {
  let component: MyRecipeComponent;
  let fixture: ComponentFixture<MyRecipeComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [MyRecipeComponent]
    });
    fixture = TestBed.createComponent(MyRecipeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});