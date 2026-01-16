import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class DataService {
  initialCards = [
    {
      id: 1,
      title: 'Random cat card',
      imageUrl: 'https://cataas.com/cat?width=250&height=200',
      description: 'That card shows a random cat image.'
    },
    {
      id: 2,
      title: 'Random cat card',
      imageUrl: 'https://cataas.com/cat/says/Hello?width=250&height=200',
      description: 'That card shows a random cat image with a text !'
    }
  ];
  constructor() { }

public initData() {
    const cards = JSON.parse(sessionStorage.getItem('cards'));
    if (!cards) {
      sessionStorage.setItem('cards', JSON.stringify(this.initialCards));
    }
  }

public getAllCards() {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      const cards = JSON.parse(sessionStorage.getItem('cards'));
      if (cards && cards.length) {
        resolve(cards);
      } else {
        reject('No cards found !');
      }
    }, 300);
  });
}


}