import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { HighScorePageComponent } from './high-score-page.component';

describe('HighScorePageComponent', () => {
  let component: HighScorePageComponent;
  let fixture: ComponentFixture<HighScorePageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ HighScorePageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(HighScorePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});