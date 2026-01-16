import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class ColorService {

  constructor() { }

  getColor(color: string, transparent: boolean = false) {

    switch (color) {
      case 'yellow':
        return transparent ? 'rgba(243, 196, 79, 0.5)' : '#f3c34c';
      case 'turquoise':
        return transparent ? 'rgba(56, 175, 174, 0.5)' : '#38afae';
      case 'red':
        return transparent ? 'rgba(239, 74, 61, 0.5)' : '#ef4a3d';
      case 'purple':
        return transparent ? 'rgba(127, 133, 215, 0.5)' : '#7f85d7';
      case 'pink':
        return transparent ? 'rgba(239, 72, 145, 0.5)' : '#ef4891';
      case 'blue':
          return transparent ? '#35568C77' : '#35568C';
      default:
        // Lila per defecte
        return transparent ? 'rgba(127, 133, 215, 0.5)' : '#7f85d7';
    }

  }

}