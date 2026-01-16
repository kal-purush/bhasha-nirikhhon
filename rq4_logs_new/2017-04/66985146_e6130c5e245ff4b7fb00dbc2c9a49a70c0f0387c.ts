/* tslint:disable:no-unused-variable */
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { DebugElement } from '@angular/core';
import { MaterialModule } from '@angular/material';
import { FormsModule } from '@angular/forms';
// USE TO NOT TEST ROUTER
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { Router } from '@angular/router';

import { LoginComponent } from './login.component';
import { AuthService } from '../../shared/auth.service';
import { AlertService } from '../../shared/alert.service';

describe('LoginComponent', () => {
  let component: LoginComponent;
  let fixture: ComponentFixture<LoginComponent>;

  class RouterStub {
    navigateByUrl(url: string) { return url; }
  }

  class AlertStub {
    message: string;
    keepAfterNavigationChange = false;
  }


  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [LoginComponent],
      imports: [MaterialModule.forRoot(), FormsModule],
      schemas: [NO_ERRORS_SCHEMA],
      providers: [
        { provide: Router, useClass: RouterStub },
        AuthService,
        { provide: AlertService, useClass: AlertStub }
      ]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LoginComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});