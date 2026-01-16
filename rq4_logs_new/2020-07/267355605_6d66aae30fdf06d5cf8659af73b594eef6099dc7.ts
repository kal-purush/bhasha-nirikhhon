import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ResetPasswordComponent } from './reset-password.component';
import { ButtonsModule } from '@tcode/buttons';
import { InputModule } from '@tcode/input';
import { RouterTestingModule } from '@angular/router/testing';
import { AUTH_CONFIG_TOKEN } from '../auth.config';

describe('ResetPasswordComponent', () => {
  let component: ResetPasswordComponent;
  let fixture: ComponentFixture<ResetPasswordComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ResetPasswordComponent ],
      imports: [
        ButtonsModule,
        InputModule,
        RouterTestingModule
      ],
      providers: [
        {
          provide: AUTH_CONFIG_TOKEN,
          useValue: {
            canResetPassword: true,
            canSignIn: true,
            canSignUp: true,
            appType: 'storefront'
          }
        }
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ResetPasswordComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});