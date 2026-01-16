import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CategoryFormModalComponent } from './category-form-modal.component';

describe('CategoryFormModalComponent', () => {
  let component: CategoryFormModalComponent;
  let fixture: ComponentFixture<CategoryFormModalComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ CategoryFormModalComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(CategoryFormModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});