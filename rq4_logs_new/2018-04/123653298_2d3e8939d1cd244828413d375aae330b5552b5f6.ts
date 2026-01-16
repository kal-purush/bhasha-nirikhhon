import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SubscriptionManageComponent } from './subscription-manage.component';

describe('SubscriptionManageComponent', () => {
  let component: SubscriptionManageComponent;
  let fixture: ComponentFixture<SubscriptionManageComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SubscriptionManageComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SubscriptionManageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});