import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DailyLoginComponent } from './daily-login.component';

describe('DailyLoginComponent', () => {
  let component: DailyLoginComponent;
  let fixture: ComponentFixture<DailyLoginComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [DailyLoginComponent]
    });
    fixture = TestBed.createComponent(DailyLoginComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});