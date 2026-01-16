import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DishDescriptionComponent } from './dish-description.component';

describe('DishDescriptionComponent', () => {
  let component: DishDescriptionComponent;
  let fixture: ComponentFixture<DishDescriptionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DishDescriptionComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DishDescriptionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});