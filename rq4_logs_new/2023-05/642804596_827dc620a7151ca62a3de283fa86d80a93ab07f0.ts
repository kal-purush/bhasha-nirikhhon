import { Injectable } from '@angular/core';
import {v4 as uuid } from'uuid';

import { Character } from '../interfaces/character.interface';

@Injectable({
    providedIn: 'root'
})

export class DbzService {

    public characters:Character[]=[{
        id:uuid(),
        name:'krilin',
        power:1000
    },{
        id:uuid(),
        name:'goku',
        power:9500
    },{
        id:uuid(),
        name:'vegeta',
        power:7500
    }];

    AddCharcter(character: Character):void{

        const newCharacter :Character = {id:uuid(),...character}
        this.characters.push(newCharacter);
    }

    // onDeleCharacter(index:number){
    //     this.characters.splice(index,1)
    deleteById(id:string){
        this.characters = this.characters.filter(character => character.id !== id)
    }


    constructor() { }
    


}