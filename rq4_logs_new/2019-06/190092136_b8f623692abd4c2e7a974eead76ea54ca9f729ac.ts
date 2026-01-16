import { Injectable } from '@angular/core';
import {Place} from './place.model';

@Injectable({
  providedIn: 'root'
})
export class PlacesService {
  private place: Place[] = [
     new Place('p1', 'Manhattan Mansion', 'in the heart of NYC', 'https://images.app.goo.gl/LJMvmSdcbRHJ8qYx8', 149.99),
    new Place('p2', 'L\'Amour Toujours', 'Romantic place in Paris', 'https://images.app.goo.gl/8ZTzP3nfZMnxwTob9', 300.99),
    new Place('p3', 'Trump Tower', 'Hotel in Manhattan', 'https://images.app.goo.gl/DfKNo3eoZDB6C4oB8', 600.99)
  ];

  get places() {
    return [...this.place];
  }
  constructor() { }
}