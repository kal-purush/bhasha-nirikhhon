import {animate, animateChild, group, query, style, transition, trigger} from '@angular/animations';

export const slideInAnimation =
  trigger('routeAnimations', [
    transition('LandingPage <=> LocationPage', [
      style({ position: 'relative' }),
      query(':enter, :leave', [
        style({
          position: 'relative',
          top: 0,
          left: 0,
          width: '100%'
        })
      ]),
      query(':enter', [
        style({ left: '-100%'})
      ]),
      query(':leave', animateChild()),
      group([
        query(':leave', [
          animate('150ms ease-in', style({ left: '100%'}))
        ]),
        query(':enter', [
          animate('150ms ease-in', style({ left: '0%'}))
        ])
      ]),
      query(':enter', animateChild()),
    ]),
  ]);