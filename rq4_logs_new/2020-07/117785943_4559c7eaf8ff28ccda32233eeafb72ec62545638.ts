import {animate, keyframes, state, style, transition, trigger} from "@angular/core";

export const homeAnnimation=[
  trigger('flip', [
    state('flipped', style({
      transform: 'rotate(180deg)',
    })),
    transition('* => flipped', animate('400ms ease'))
  ]),

  trigger('flyInOut', [
    state('in', style({
      transform: 'translate3d(0, 0, 0)'
    })),
    state('out', style({
      transform: 'translate3d(-35%, 0, 0)'
    })),
    transition('in => out', animate('200ms ease-in')),
    transition('out => in', animate('200ms ease-out'))
  ]),

  trigger('fade', [
    state('visible', style({
      opacity: 1
    })),
    state('invisible', style({
      opacity: 0.1
    })),
    transition('visible <=> invisible', animate('200ms linear'))
  ]),

  trigger('bounce', [
    state('bouncing', style({
      transform: 'translate3d(0,0,0)'
    })),
    transition('* => bouncing', [
      animate('900ms ease-in', keyframes([
        style({ transform: 'translate3d(0,0,0)', offset: 0 }),
        style({ transform: 'translate3d(0,-30px,0)', offset: 0.5 }),
        style({ transform: 'translate3d(0,0,0)', offset: 1 })
      ]))
    ])
  ])

]

export  const concoursDetailsAnimation=[

  trigger('flip', [
    state('flipped', style({
      transform: 'rotate(180deg)',
      backgroundColor: '#f50e80'
    })),
    transition('* => flipped', animate('400ms ease'))
  ]),

  trigger('flyInOut', [
    state('in', style({
      transform: 'translate3d(0, 0, 0)'
    })),
    state('out', style({
      transform: 'translate3d(150%, 0, 0)'
    })),
    transition('in => out', animate('200ms ease-in')),
    transition('out => in', animate('200ms ease-out'))
  ]),

  trigger('fade', [
    state('visible', style({
      opacity: 1
    })),
    state('invisible', style({
      opacity: 0.1
    })),
    transition('visible <=> invisible', animate('200ms linear'))
  ]),

  trigger('bounce', [
    state('bouncing', style({
      transform: 'translate3d(0,0,0)'
    })),
    transition('* => bouncing', [
      animate('900ms ease-in', keyframes([
        style({transform: 'translate3d(0,0,0)', offset: 0}),
        style({transform: 'translate3d(0,-30px,0)', offset: 0.5}),
        style({transform: 'translate3d(0,0,0)', offset: 1})
      ]))
    ])
  ])

]