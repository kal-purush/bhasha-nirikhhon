import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PostTripInspectionComponent } from './post-trip-inspection.component';

describe('PostTripInspectionComponent', () => {
  let component: PostTripInspectionComponent;
  let fixture: ComponentFixture<PostTripInspectionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PostTripInspectionComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PostTripInspectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});