import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NoGamesComponent } from './no-games.component';

describe('NoGamesComponent', () => {
  let component: NoGamesComponent;
  let fixture: ComponentFixture<NoGamesComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NoGamesComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NoGamesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});