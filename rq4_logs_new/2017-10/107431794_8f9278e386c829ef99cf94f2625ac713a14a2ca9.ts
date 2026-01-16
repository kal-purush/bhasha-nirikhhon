import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ScoreboardTeamCtrlComponent } from './scoreboard-team-ctrl.component';

describe('ScoreboardTeamCtrlComponent', () => {
  let component: ScoreboardTeamCtrlComponent;
  let fixture: ComponentFixture<ScoreboardTeamCtrlComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ScoreboardTeamCtrlComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ScoreboardTeamCtrlComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});