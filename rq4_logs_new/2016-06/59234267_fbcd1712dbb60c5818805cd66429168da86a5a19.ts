import {NavController, Toast} from "ionic-angular/index";
import {ToastOptions} from "ionic-native/dist/plugins/toast";
/**
 * Created by tom on 07.06.16.
 */

export abstract class ErrorHandler {
    constructor(protected nav: NavController) {
        
    }

    protected handleError(error:Error) {
        this.nav.present(Toast.create({message: error.message, duration: 3000}));
    }
    
    protected handleErrorString(error:string) {
        this.nav.present(Toast.create({message: error, duration: 3000}));
    }
}