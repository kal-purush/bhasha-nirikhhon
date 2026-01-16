import { NavController, Page, ActionSheet } from 'ionic-framework/ionic';

@Page({ templateUrl: 'build/pages/addspider/addspider.html' })
export class AddSpiderPage {

    constructor(private nav: NavController) { }

    save(event: UIEvent) {
        console.log('SAVING SPIDER');
        console.dir(event);
    }

    takePicture() {
        let actionSheet = ActionSheet.create({
            buttons: [
                { text: 'Take Photo...', handler: () => this.getPicture(Camera.PictureSourceType.CAMERA) },
                { text: 'Choose from Library...', handler: () => this.getPicture(Camera.PictureSourceType.PHOTOLIBRARY) },
                { text: 'Cancel', style: 'cancel' }]
        });
        this.nav.present(actionSheet);
    }

    private getPicture(source) {
        console.log('should be getting picture from', source);
    }

}