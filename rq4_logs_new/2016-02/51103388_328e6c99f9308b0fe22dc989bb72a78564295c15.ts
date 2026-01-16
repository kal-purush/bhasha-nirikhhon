import {Component} from 'angular2/core';
import {TokenService} from '../../services/token-service';

@Component({
    selector: 'etik-tak-main',
    template: `
        <p>{{ stateText }}</p>
    `
})

export class HomeComponent {

    stateText: string = this.tokenService.getToken();

    constructor(private tokenService: TokenService) {}
}