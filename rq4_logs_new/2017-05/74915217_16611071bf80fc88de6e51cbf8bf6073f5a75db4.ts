import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DemoRatingComponent } from './demo-rating.component';

describe('DemoRatingComponent', () => {
  let component: DemoRatingComponent;
  let fixture: ComponentFixture<DemoRatingComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DemoRatingComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DemoRatingComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});