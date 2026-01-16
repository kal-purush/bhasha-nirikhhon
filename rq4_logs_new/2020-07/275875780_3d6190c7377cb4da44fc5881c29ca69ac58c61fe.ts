import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { HomeFollowingTweetsComponent } from './home-following-tweets.component';

describe('HomeFollowingTweetsComponent', () => {
  let component: HomeFollowingTweetsComponent;
  let fixture: ComponentFixture<HomeFollowingTweetsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ HomeFollowingTweetsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(HomeFollowingTweetsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});