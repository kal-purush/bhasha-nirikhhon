import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NavBarNotificationComponent } from './nav-bar-notification.component';

describe('NavBarNotificationComponent', () => {
  let component: NavBarNotificationComponent;
  let fixture: ComponentFixture<NavBarNotificationComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NavBarNotificationComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NavBarNotificationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});