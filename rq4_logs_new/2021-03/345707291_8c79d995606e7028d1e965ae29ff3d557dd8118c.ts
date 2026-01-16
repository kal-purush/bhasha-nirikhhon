import {
  trigger,
  state,
  style,
  animate,
  transition, keyframes,
  // ...
} from '@angular/animations';


export let moveLetter = trigger('moveLetter', [
  // state('false', style({opacity: 1, filter: 'blur(0)', transform: 'translateY(0) translateX(0) rotate(0deg)'})),
  // state('move', style({
  //
  // })),
  transition('false => move', [
    animate('2s', keyframes([
      style({filter: 'blur(0)', transform: 'translateY(0) translateX(0) rotate(0deg)', offset: 0}),
      // style({ offset: 0.2}),
      style({ filter: 'blur(5px)',
        transform: 'translateY(-200px) translateX(300px) rotate(720deg) scale(4)', offset: 0.3}),
      style({filter: 'blur(10px)',
        transform: 'translateY(-300px) translateX(300px) rotate(720deg) scale(4)', offset: 1})
    ]))

    // animate(500, style({ opacity: 0, filter: 'blur(10px)', transform: 'transform: translateY(-300px) translateX(300px) rotate(720deg) scale(4)' }))
  ]),
]);