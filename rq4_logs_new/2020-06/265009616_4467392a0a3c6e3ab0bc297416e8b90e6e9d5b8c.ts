import {
    animation, animate, trigger, animateChild, group,
    transition, style, query
} from '@angular/animations';

export const scaleAnimation = animation([
    style({
        opacity: '{{ opacityFrom }}',
        scale: '{{ scaleFrom }}'
    }),
    animate('{{ time }}', style({ opacity: '{{ opacityTo }}', scale: '{{ scaleTo }}' }))
]);