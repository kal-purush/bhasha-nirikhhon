import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ButtonSelectCategoryComponent } from './button-select-category.component';

describe('ButtonSelectCategoryComponent', () => {
  let component: ButtonSelectCategoryComponent;
  let fixture: ComponentFixture<ButtonSelectCategoryComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ButtonSelectCategoryComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ButtonSelectCategoryComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});