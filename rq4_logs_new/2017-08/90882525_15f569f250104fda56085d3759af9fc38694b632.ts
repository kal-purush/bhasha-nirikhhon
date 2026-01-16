import {  trigger,
          state,
          style,
          transition,
          animate,
          keyframes,
          group
       } from '@angular/animations';

export const PlaySelectAnimation = [
  trigger('battingButton', [
    transition('* => *', [
      animate(1000, keyframes([
        style({opacity: 0, transform: 'translateY(500%)', offset: 0}),
        style({opacity: 1, transform: 'translateY(0)',     offset: 0.5})
      ]))
    ])
  ]),
  trigger('bowlingButton', [
    transition('* => *', [
      animate(1000, keyframes([
        style({opacity: 0, transform: 'translateY(500%)', offset: 0}),
        style({opacity: 1, transform: 'translateY(500%)', offset: 0.4}),
        style({opacity: 1, transform: 'translateY(0)',     offset: 0.8})
      ]))
    ])
  ]),
  trigger('tossCoinButton', [
    transition('* => *', [
      animate(1000, keyframes([
        style({opacity: 0, transform: 'translateY(500%)', offset: 0}),
        style({opacity: 1, transform: 'translateY(500%)', offset: 0.7}),
        style({opacity: 1, transform: 'translateY(0)',     offset: 1})
      ]))
    ])
  ])
];