import { Injectable } from '@angular/core';
import { Region } from '../interfaces/country.interfaces';

@Injectable({
  providedIn: 'root',
})
export class CountriesService {
  private _regions: Region[] = [
    //el guión es para indicar que es una propiedad privada y que nadie la modifique
    Region.Africa,
    Region.Americas,
    Region.Asia,
    Region.Europe,
    Region.Oceania,
  ];

  constructor() {}

  get regions(): Region[] {
    return [...this._regions]; // los ... mandan una copia del array y no el original para que no se pueda modificar el original
  }
}