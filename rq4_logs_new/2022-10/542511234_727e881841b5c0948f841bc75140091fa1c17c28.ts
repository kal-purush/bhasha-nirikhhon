import { Injectable } from '@angular/core';
import ads from './ads.json';

@Injectable({
  providedIn: 'root'
})
export class AdService {
  //TODO: not random but rather in order to avoid same ads repeating
  get ad(): string {
    return ads.funFacts[Math.floor(Math.random() * ads.funFacts.length)];
  }
}