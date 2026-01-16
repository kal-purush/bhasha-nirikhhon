import { Component } from '@angular/core';

@Component({
  selector: 'earth-exploded-message',
  templateUrl: './earth-exploded-message.component.html',
  styleUrls: ['./earth-exploded-message.component.css']
})
export class EarthExplodedMessageComponent {

  saveThePlanet(event) {
    alert("planet saved");
  }
}