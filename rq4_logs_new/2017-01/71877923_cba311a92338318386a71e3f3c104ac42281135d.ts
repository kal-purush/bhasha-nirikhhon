import {Injectable} from '@angular/core';
import {IMyLocales, IMyOptions} from '../interfaces/index';

@Injectable()
export class LongLabelService {
    private longLabels:IMyLocales = {
        'en': {
            dayLabels: {su: 'Sunday', mo: 'Monday', tu: 'Tuesday', we: 'Wednesday', th: 'Thursday', fr: 'Friday', sa: 'Saturday'},
            monthLabels: { 1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June', 7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December' }
        },
        'fr': {
            dayLabels: {su: 'Dimanche', mo: 'Lundi', tu: 'Mardi', we: 'Mercredi', th: 'Jeudi', fr: 'Vendredi', sa: 'Samedi'},
            monthLabels: {1: 'Janvier', 2: 'Février', 3: 'Mars', 4: 'Avril', 5: 'Mai', 6: 'Juin', 7: 'Juillet', 8: 'Août', 9: 'Septembre', 10: 'Octobre', 11: 'Novembre', 12: 'Décembre'}
        }
    };

    getLongLabels(locale:string): IMyOptions {
        if (locale && this.longLabels.hasOwnProperty(locale)) {
            // User given locale
            return this.longLabels[locale];
        }
        // Default: en
        return this.longLabels['en'];
    }
}