import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { IndividualLeaderboardComponent } from './individual-leaderboard.component';

describe('IndividualLeaderboardComponent', () => {
  let component: IndividualLeaderboardComponent;
  let fixture: ComponentFixture<IndividualLeaderboardComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ IndividualLeaderboardComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(IndividualLeaderboardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});