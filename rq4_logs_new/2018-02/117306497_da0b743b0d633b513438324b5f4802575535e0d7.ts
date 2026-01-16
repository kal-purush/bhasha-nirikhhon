import {
    Component,
    Input
} from '@angular/core';
import {
    trigger,
    state,
    style,
    animate,
    transition
} from '@angular/animations';

export const pageChangeTime = 300;

export const pageAnimation = [
    trigger('imageLoading', [
        state('true', style({
            filter: 'brightness(0)',
            opacity: 0.4
        })),
        state('false', style({
            filter: 'brightness(1)',
            opacity: 1
        })),
        transition('true => false', animate(pageChangeTime + 'ms ease-in')),
        transition('false => true', animate(pageChangeTime + 'ms ease-out'))
    ])
];