import { animate, state, style, transition, trigger } from '@angular/animations';




export const coverAnim = trigger('coverAnim', [
  state('inactive', style({
    transform: 'scale(.75)'
  })),
  state('active', style({
    transform: 'scale(1)'
  })),
  transition('inactive => active', animate('200ms 500ms ease-in-out')),
  transition('active => inactive', animate('300ms 150ms ease-in-out'))
]);

export const titleAnim = trigger('titleAnim', [
  state('inactive', style({
    transform: 'translateY(-50%)',
    opacity: '0'
  })),
  state('active', style({
    transform: 'translateY(0%)',
    opacity: '1'
  })),
  transition('inactive => active', animate('200ms 800ms ease-in-out')),
  transition('active => inactive', animate('200ms ease-in-out'))
]);

export const authorAnim = trigger('authorAnim', [
  state('inactive', style({
    transform: 'translateY(-50%)',
    opacity: '0'
  })),
  state('active', style({
    transform: 'translateY(0%)',
    opacity: '1'
  })),
  transition('inactive => active', animate('200ms 850ms ease-in-out')),
  transition('active => inactive', animate('200ms ease-in-out'))
]);