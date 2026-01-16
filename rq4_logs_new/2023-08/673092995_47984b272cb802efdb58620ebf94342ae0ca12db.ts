import { ComponentFixture, TestBed } from '@angular/core/testing';

import { LeagueDetailComponent } from './league-detail.component';

describe('LeagueDetailComponent', () => {
  let component: LeagueDetailComponent;
  let fixture: ComponentFixture<LeagueDetailComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [LeagueDetailComponent]
    });
    fixture = TestBed.createComponent(LeagueDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});