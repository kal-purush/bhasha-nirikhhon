import {Observable, fromEvent} from "rxjs";

document.addEventListener('DOMContentLoaded', () => {
  const inputText = document.getElementById('ejemplosInput');

  const observable = fromEvent(inputText, 'keyup');

  observable.subscribe((e) => {
    console.log('keyup', e.target.value);
    inputText.textContent = "asd";
  });
});
