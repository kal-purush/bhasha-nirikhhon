import {  trigger,
          state,
          style,
          transition,
          animate,
          keyframes,
          group
       } from '@angular/animations';

export const GameAnimation = [
  trigger('runs', [
    transition('* => *', [
      animate(300, keyframes([
        style({opacity: 0, transform: 'translateY(100%)', offset: 0}),
        style({opacity: 1, transform: 'translateY(-20px)',  offset: 0.3}),
        style({opacity: 1, transform: 'translateY(0)',     offset: 1.0})
      ]))
    ])
  ]),
  trigger('played', [
    transition('* => *', [
      animate(400, keyframes([
        style({opacity: 0, transform: 'translateX(-100%)', offset: 0}),
        style({opacity: 1, transform: 'translateX(20px)',  offset: 0.3}),
        style({opacity: 1, transform: 'translateX(0)',     offset: 1.0})
      ]))
    ])
  ]),
  trigger('out', [
    transition('* => *', [
      animate(500, keyframes([
        style({opacity: 0, transform: 'translateY(-100%)', offset: 0}),
        style({opacity: 1, transform: 'translateY(20px)',  offset: 0.3}),
        style({opacity: 1, transform: 'translateY(-20px)',  offset: 0.6}),
        style({opacity: 1, transform: 'translateX(0)',     offset: 1.0})
      ]))
    ])
  ]),
  trigger('wins', [
    transition('* => *', [
      animate(500, keyframes([
        style({opacity: 0, transform: 'translateY(100%)', offset: 0}),
        style({opacity: 1, transform: 'translateY(-20px)',  offset: 0.3}),
        style({opacity: 1, transform: 'translateY(20px)',  offset: 0.6}),
        style({opacity: 1, transform: 'translateX(0)',     offset: 1.0})
      ]))
    ])
  ])
];