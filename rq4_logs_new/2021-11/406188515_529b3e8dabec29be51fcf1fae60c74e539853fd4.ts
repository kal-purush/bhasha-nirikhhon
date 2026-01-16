import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ActivityThreeComponent } from './activity-three.component';

describe('ActivityThreeComponent', () => {
  let component: ActivityThreeComponent;
  let fixture: ComponentFixture<ActivityThreeComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ActivityThreeComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ActivityThreeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});