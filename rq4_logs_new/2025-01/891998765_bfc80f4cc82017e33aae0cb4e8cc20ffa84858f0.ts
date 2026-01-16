import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmsRegistrationComponent } from './sms-registration.component';

describe('SmsRegistrationComponent', () => {
  let component: SmsRegistrationComponent;
  let fixture: ComponentFixture<SmsRegistrationComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SmsRegistrationComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(SmsRegistrationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});