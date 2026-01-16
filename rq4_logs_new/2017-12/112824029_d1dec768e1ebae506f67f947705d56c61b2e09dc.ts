import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ActivityCategoryDetailComponent } from './activity-category-detail.component';

describe('ActivityCategoryDetailComponent', () => {
  let component: ActivityCategoryDetailComponent;
  let fixture: ComponentFixture<ActivityCategoryDetailComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ActivityCategoryDetailComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ActivityCategoryDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});