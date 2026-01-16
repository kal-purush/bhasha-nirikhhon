import { Injectable } from '@angular/core';
import { MatDialog } from '@angular/material';
import { ActivatedRouteSnapshot, CanDeactivate, RouterStateSnapshot } from '@angular/router';
import { ConfirmDialog } from '../common/dialogs/confirm.dialog';
import { ItemDetailsComponent } from './item-details.component';

@Injectable()
export class CanDeactivateManagerGuard implements CanDeactivate<ItemDetailsComponent> {
	constructor(private dialog: MatDialog) {
	}

	canDeactivate(
		component: ItemDetailsComponent,
		route: ActivatedRouteSnapshot,
		state: RouterStateSnapshot
	) {
		if (component.canDeactivate()) {
			return true;
		}

		let data = {
			title: 'Do you want to leave this site?',
			message: 'Changes you made may not be saved',
			buttonConfirm: 'Leave',
			buttonCancel: 'Stay'
		};
		return this.dialog.open(ConfirmDialog, { data: data })
			.afterClosed()
			.map(x => x === 'OK');
	}
}