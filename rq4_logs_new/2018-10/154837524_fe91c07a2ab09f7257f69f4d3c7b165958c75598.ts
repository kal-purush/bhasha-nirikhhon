import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class RandomService {

  constructor() { }

  getRandomNumbers(min: number, max: number, quantity: number) : number[] {
    let numbers = [];
    while(numbers.length < quantity){
      let rand = this.getRandomNumber(min, max);
      if(numbers.indexOf(rand) < 0){
        numbers.push(rand);
      }
    }
    return numbers;
  }

  getRandomNumber(min: number, max: number) : number{
    let range = max - min;
    let rand = Math.floor(Math.random() * range);
    return rand + min;
  }
}