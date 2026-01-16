import { ComponentFixture, TestBed } from '@angular/core/testing';

import { FilterDialogBoxComponent } from './filter-dialog-box.component';

describe('FilterDialogBoxComponent', () => {
  let component: FilterDialogBoxComponent;
  let fixture: ComponentFixture<FilterDialogBoxComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [FilterDialogBoxComponent]
    });
    fixture = TestBed.createComponent(FilterDialogBoxComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});