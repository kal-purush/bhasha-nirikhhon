import { Component } from '@angular/core';
import { Alert, Loading, NavController, Toast, ViewController } from 'ionic-angular';

/*
  Generated class for the BuyerLookingforModalPage page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  templateUrl: 'build/pages/buyer-lookingfor-modal/buyer-lookingfor-modal.html',
})
export class BuyerLookingforModalPage {
	maxCharacterLimit = 140;
    remainingCharacters = 140;
    emote = {};

	constructor(
		private nav: NavController,
		private view: ViewController
	) {}

	characterCounter(value) {
        if (!value || value.length === 0 ) {
            this.remainingCharacters = this.maxCharacterLimit;
        }

        // compute
        this.remainingCharacters = this.maxCharacterLimit - value.length;
    }

    /**
     * Closes the modal
     */
    dismiss() {
        this.view.dismiss();
    }

    /**
     * Render and shows a toast message
     */
    showToast(message) {
        let toast = Toast.create({
            message: message,
            duration: 3000
        });

        // render in the template
        this.nav.present(toast);
    }

    /**
     * Sends out the emote to nearby sellers.
     */
    submitEmote(emoteForm) {
        if (!emoteForm.valid) {
            // prompt that something is wrong in the form
            let alert = Alert.create({
                title: 'Ooops...',
                subTitle: 'Something is wrong. Make sure the form fields are properly filled in.',
                buttons: ['OK']
            });

            // render in the template
            this.nav.present(alert);
            return;
        }

        // initialize the loader
        let loading = Loading.create({
            content: 'Sending out your emote...'
        });

        // render in the template
        this.nav.present(loading);

        // TODO: add thingy here
        setTimeout(() => {
            // dismiss the loader
            loading.dismiss().then(() => {
                // close the modal
                this.dismiss();
            })
            .then(() => {
                // delay it for a second
                setTimeout(() => {
                    // show a toast
                    this.showToast('You have successfully sent out your emote.');
                }, 400);
            });
        }, 3000);
    }

}