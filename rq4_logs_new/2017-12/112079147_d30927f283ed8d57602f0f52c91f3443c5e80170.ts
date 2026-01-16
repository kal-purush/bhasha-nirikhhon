import {Component, OnInit} from '@angular/core';
import {UserEmailResendConfirmationRequest} from '../../../nutrition-buddy/identity/models/user.model';
import {FrontendRouteIds} from '../../globals/frontend-route-ids';
import {RoutingService} from '../../services/routing.service';
import {UserRepositoryService} from '../../../nutrition-buddy/identity/services/user-repository.service';
import {ActivatedRoute} from '@angular/router';
import {AlertService} from '../../services/alert.service';

@Component({
  selector: 'app-email-confirm',
  templateUrl: './email-confirm.component.html',
  styleUrls: ['./email-confirm.component.less']
})
export class EmailConfirmComponent implements OnInit {

  emailIsConfirmed: boolean;
  confirmationStatusText: string;

  private _email;
  private _status: number;
  private _statusSuccess = 'Confirmation was successful!';
  private _statusFailed = 'Oops, confirmation failed. It is either expired or something went wrong on our side.';


  constructor(private _route: ActivatedRoute,
              private _routingService: RoutingService,
              private _alertService: AlertService,
              private _userRepositoryService: UserRepositoryService) {
  }

  ngOnInit() {
    this.updateConfirmationStatus();
  }

  updateConfirmationStatus(): void {
    this._email = this._route.snapshot.paramMap.get('email');
    this._status = +this._route.snapshot.paramMap.get('status');
    this.emailIsConfirmed = this._status === 1;
    this.confirmationStatusText = this._status === 1 ? this._statusSuccess : this._statusFailed;
  }

  onResendConfirmationMail(): void {
    const request = new UserEmailResendConfirmationRequest(this._email);
    this._userRepositoryService.resendConfirmationEmail(request)
      .then(result => {
        this._alertService.displayMessage('Confirmation email was re-sent');
        this._routingService.navigateTo(FrontendRouteIds.Login);
      })
      .catch(response => {
        this._routingService.navigateTo(FrontendRouteIds.Login);
      });
  }

  onLoginClick(): void {
    this._routingService.navigateTo(FrontendRouteIds.Login);
  }
}