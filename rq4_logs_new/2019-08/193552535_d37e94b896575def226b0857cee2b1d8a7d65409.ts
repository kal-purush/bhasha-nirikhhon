import {Component, Input} from '@angular/core';

@Component({
	selector: 'app-confirm-notification',
	templateUrl: './confirm-notification.component.html',
	styleUrls: ['./confirm-notification.component.scss']
})
export class ConfirmNotificationComponent {
	@Input() notificationMessage: string;
	@Input() openNotification: boolean;
	@Input() error = null;

	hideError() {
		this.error = null;
	}

	hideNotification() {
		this.openNotification = false;
	}
}