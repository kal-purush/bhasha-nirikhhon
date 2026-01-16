import { Injectable } from '@angular/core';
import { Router } from '@angular/router';
import { MatDialog, MatDialogConfig } from '@angular/material';

import { CMSService } from '@app/services/cms.service';
import { ModalData, CmsContent } from '@app/models';



import { ModalComponent } from '@app/modules/shared-module/modals/modal.component';


@Injectable()
export class ModalService {

	constructor(
		private dialog: MatDialog,
		private router: Router,
		private cmsService: CMSService) { }


	/**
	 * Opens a delete content modal for the given content item
	 * @param content
	 */
	public openDeleteContentModal(content: CmsContent) {
		const data: ModalData = {
			headerText: `Delete ${content.title}`,
			bodyText: 'Do you wish to proceed?',
			proceedText: 'Delete', proceedColor: 'warn',
			cancelText: 'Cancel', cancelColor: 'accent',
		};
		return this.dialog.open(ModalComponent, <MatDialogConfig>{ data: data });
	}

	/**
	 * Opens a deactivation warning modal
	 */
	public openDeactivateComposeModal() {
		const data: ModalData = {
			headerText: 'Unsaved work!',
			bodyText: 'Do you wish to proceed without saving?',
			proceedColor: 'accent', proceedText: 'Proceed',
			cancelColor: 'primary', cancelText: 'Cancel',
		};
		return this.dialog.open(ModalComponent, <MatDialogConfig>{ data: data });
	}

	/**
	 * Opens a restore old version modal
	 * @param version
	 */
	public openRestoreOldVersionModal(version: string) {
		const data: ModalData = {
			headerText: `Restore version ${version}`,
			bodyText: 'Do you wish to replace your current draft with this version?',
			proceedText: 'Proceed', proceedColor: 'warn',
			cancelText: 'Cancel',
		};

		return this.dialog.open(ModalComponent, <MatDialogConfig>{ data: data });
	}

	/**
	 * Opens a password change success state modal
	 * @param success
	 */
	public openPasswordChangeModal(success: boolean) {
		const data: ModalData = {
			headerText: success ? 'Password updated!' : 'Failure',
			bodyText: success ? 'Your password was successfully updated.'
							  : 'Could not update your password.',

			proceedColor: 'primary',
			proceedText: 'Okay',
		};
		return this.dialog.open(ModalComponent, <MatDialogConfig>{ data: data });
	}
}