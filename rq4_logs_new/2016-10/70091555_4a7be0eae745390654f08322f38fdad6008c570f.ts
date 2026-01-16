import { Component } from '@angular/core';
import { SessionService } from '../session/session.service';
import { ModalService } from '../modal/modal.service';
import { ForgotPasswordService } from './forgot-password.service';
import { ErrorService } from '../error/error.service';
import { Router } from '@angular/router';

@Component({
  selector: 'app-forgot-password',
  template: `
        <div class="container">
            <div class="card">
                <div class="card-content">
                    <span class="card-title">Forgot Password</span>
                    <br />
                    <p class="instructions">
                        Enter the email address for your account and you will receive instructions to reset your password.
                    </p>
                    <br />
                    <form (submit)="resetPassword(email.value)">
                        <label for="email">Email Address</label>
                        <input type="email" id="email" #email>
                        <button class="btn cxo-btn-primary" type="submit">Reset Password</button>
                    </form>
                </div>
            </div>
            <div class="right-align">
                <a class="cxo-link" routerLink="/login">Back to Login</a>
            </div>
        </div>
    `,
  styles: [`
    .instructions {
        margin-top: 15px !important;
    }
  `],
  providers: [ForgotPasswordService]
})
export class ForgotPasswordComponent {

    constructor (
        private sessionService: SessionService,
        private modalService: ModalService,
        private forgotPasswordService: ForgotPasswordService,
        private errorService: ErrorService,
        private router: Router) {
            if (this.sessionService.isLoggedIn()) {
                this.router.navigate(['/cash_flow']);
            }
        }

    isLoggedIn() {
        return this.sessionService.isLoggedIn();
    }

    resetPassword(email) {
        this.forgotPasswordService.forgotPassword(email).subscribe(res => {
            if (res.json().messages === 'Email has been sent.') {
                this.modalService.openModal(
                    'Password Reset',
                    '<p>Instructions to reset your password have been sent to ' + email + '.</p>',
                    null);
            }
        }, err => {
            this.errorService.handle(err);
        });
    }
}