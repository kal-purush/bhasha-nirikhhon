import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AccountRecoveryPageComponent } from './account-recovery-page.component';

describe('AccountRecoveryPageComponent', () => {
  let component: AccountRecoveryPageComponent;
  let fixture: ComponentFixture<AccountRecoveryPageComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [AccountRecoveryPageComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(AccountRecoveryPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});