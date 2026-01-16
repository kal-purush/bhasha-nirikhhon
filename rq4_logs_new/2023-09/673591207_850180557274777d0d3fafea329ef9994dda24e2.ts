import { Injectable } from '@angular/core';
import { v4 as uuid } from 'uuid';
import { Character } from '../interfaces/character.interface';


@Injectable({providedIn: 'root'})
export class DBZService {
  public characters: Character[] = [
    {
      id: uuid(),
      name: 'Krillin',
      power: 3000
    },
    {
      id: uuid(),
      name: 'Goku',
      power: 10000
    },
    {
      id: uuid(),
      name: 'Piccoro',
      power: 7000
    },
    {
      id: uuid(),
      name: 'Gohan',
      power: 4000
    }
  ];


  addCharacter(character: Character ): void {
    this.characters.push(character);
  };

  deleteCharacterById(id: string): void {
    if(!id) return;
    this.characters = this.characters.filter((character) => character.id !== id);
    console.log('index deleted', id)
  };
}