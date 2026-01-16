import { Injectable } from '@angular/core';
import { GIFTS } from './gift-manifest';

const NUM_NEW_USER_GIFTS = 3;

@Injectable()
export class CurrencyService {

  constructor() { }

  generateNewUserItems() {
    const weights = getFirstNFibNums(GIFTS.length);
    const weightedItems = getWeightedItems(GIFTS, weights);
    const randomItems = getRandomItems(weightedItems, NUM_NEW_USER_GIFTS);
    console.log(randomItems);
    return randomItems;
  }

}

const getFirstNFibNums = n => {
  const weights = [];
  let a = 1;
  let b = 0;
  let temp;
  while (n - 1 >= 0) {
    temp = a;
    a = a + b;
    b = temp;
    weights.push(b);
    n--;
  }
  return weights;
};

const getWeightedItems = (gifts, weights) => {
  const weightedItems = [];
  for (let i = 0; i <  gifts.length; i++) {
    const gift = gifts[i];
    const weight = weights[i];
    for (let j = 0; j < weight; j++) {
      weightedItems.push(gift);
    }
  }
  return weightedItems;
};

const getRandomItems = (arr, numItems) => {
  const randomItems = [];
  for (let j = 0; j < numItems; j++) {
    const randomItem = arr[Math.floor(Math.random() * arr.length)];
    randomItems.push(randomItem);
  }
  return randomItems;
};