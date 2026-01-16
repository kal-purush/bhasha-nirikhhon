import { Directive, Output, EventEmitter, ElementRef, NgZone } from '@angular/core';
import { MapsAPILoader } from '@agm/core';

@Directive({
  selector: '[aeGmapsAutocomplete]'
})

export class GmapsAutocompleteDirective {
  @Output() placeChanged: EventEmitter<google.maps.LatLng> = new EventEmitter<google.maps.LatLng>();

  constructor(private el: ElementRef, private mapsLoader: MapsAPILoader, private ngZone: NgZone) {
    this.mapsLoader.load().then(() => { 
      const autocomplete = new google.maps.places.Autocomplete(el.nativeElement); 

      autocomplete.addListener('place_changed', () => {
        const place = autocomplete.getPlace();

        if (place.geometry && place.geometry.location) {
          this.ngZone.run(() => {
            this.placeChanged.emit(place.geometry.location)
          });

        }
      });
    });
  }

}