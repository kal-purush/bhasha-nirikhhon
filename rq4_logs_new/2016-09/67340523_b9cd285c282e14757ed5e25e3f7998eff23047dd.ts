import {
    Component
} from '@angular/core'

import {
    Router
} from '@angular/router'


@Component({
    selector: 'home',
    template: require('./home.html'),
})
export class HomeComponent {
    constructor(private router) {

    }
    isplayDeatils() {
        this.router.navigateByUrl('(details:details-items;id:1)')
    }
}

HomeComponent['parameters'] = [Router]