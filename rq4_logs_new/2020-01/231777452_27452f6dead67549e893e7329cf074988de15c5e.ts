import { Component, OnInit } from '@angular/core';
import { Property } from '../../models/property';
import { PropertiesService } from './services/properties.service';

@Component({
    templateUrl: './properties.component.html'
})

export class PropertiesComponent implements OnInit {
    constructor(private propertiesService: PropertiesService) { }

     testowaZmienna = 'stronie';

    ngOnInit(): void{
      
        this.propertiesService.getProperties().subscribe(
            props => { console.log(props); },
            error => {console.log(error)}
        );
    }
}