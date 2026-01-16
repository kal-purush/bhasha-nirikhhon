import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { LastAchievementsListComponent } from './last-achievements-list.component';

describe('LastAchievementsListComponent', () => {
  let component: LastAchievementsListComponent;
  let fixture: ComponentFixture<LastAchievementsListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LastAchievementsListComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LastAchievementsListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});