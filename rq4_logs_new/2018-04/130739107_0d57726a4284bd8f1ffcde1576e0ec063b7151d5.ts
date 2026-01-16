import { animate, state, style, transition, trigger } from '@angular/animations';

export const addBoxAnim = trigger('addBoxAnim', [
  state('inactive', style({
    left: '0%',
    transform: 'translateX(0%)',
    width: '4rem',
    height: '2.25rem'
  })),
  state('search', style({
    left: '50%',
    transform: 'translateX(-50%)',
    width: '20rem',
    height: '2.25rem',
    boxShadow: '0 0 15px rgba(0, 0, 0, .3)'
  })),
  state('show', style({
    left: '50%',
    transform: 'translateX(-50%)',
    width: '20rem',
    height: '30rem',
    boxShadow: '0 0 15px rgba(0, 0, 0, .3)'
  })),
  transition('inactive <=> search', animate('200ms ease-in-out')),
  transition('search => show', animate('200ms ease-in-out'))
]);