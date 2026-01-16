import { Component } from '@angular/core';

@Component({
    selector: 'app-parks',
    templateUrl: 'parks.component.html',
    styleUrls: ['parks.component.scss']
})

export class ParksComponent {
    public name: string;
    public height: number;
    public vegetation: string;
    public status: boolean;

    constructor() {
        this.name = 'Natural park for horses';
        this.height = 450;
        this.vegetation = 'Alta';
        this.status = true;
    }
}