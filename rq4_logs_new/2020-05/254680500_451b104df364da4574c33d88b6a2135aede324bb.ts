import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { LastPlayedGamesComponent } from './last-played-games.component';

describe('LastPlayedGamesComponent', () => {
  let component: LastPlayedGamesComponent;
  let fixture: ComponentFixture<LastPlayedGamesComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LastPlayedGamesComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LastPlayedGamesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});