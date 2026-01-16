import React from "react";

export const BubbleSort = (array, setSteps) => {
  let funarray = new Array();
  funarray = [...array];
  let temp = [];
  setSteps([]);
  for (let i = 0; i < funarray.length - 1; i++) {
    for (let j = 0; j < funarray.length - 1; j++) {
      if (funarray[j] > funarray[j + 1]) {
        [funarray[j], funarray[j + 1]] = [funarray[j + 1], funarray[j]];
        temp.push(funarray.slice());
      }
    }
  }
  console.log("qorking");
  console.log(temp);
  setSteps(temp);

  return funarray;
};

export const InsertionSort = (array, setSteps) => {
  let funarray = new Array();
  funarray = [...array];
  let temp = [];
  setSteps([]);
  for (let i = 1; i < funarray.length; i++) {
    let j = i - 1;
    let x = funarray[i];
    while (j > -1 && funarray[j] > x) {
      funarray[j + 1] = funarray[j];
      j--;
    }
    funarray[j + 1] = x;
    temp.push(funarray.slice());
  }
  setSteps(temp);
};
export const SelectionSort = (array, setSteps) => {
  let funarray = new Array();
  funarray = [...array];
  let temp = [];
  setSteps([]);
  for (let i = 0; i < funarray.length; i++) {
    let j, k;
    for (j = k = i; j < funarray.length; j++) {
      if (funarray[j] < funarray[k]) {
        k = j;
      }
    }
    [funarray[i], funarray[k]] = [funarray[k], funarray[i]];

    temp.push(funarray.slice());
  }
  setSteps(temp);
};
export const HeapSort = () => {};
export const MergeSort = (array, setSteps, low, high) => {
  let funarray = new Array();
  funarray = [...array];
  let temp = [];
  setSteps([]);
};
export const QuickSort = () => {};