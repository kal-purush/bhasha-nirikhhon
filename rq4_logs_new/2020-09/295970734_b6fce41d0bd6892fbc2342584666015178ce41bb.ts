import { trigger, state, style, transition, animate, } from '@angular/animations';

export const slideInAnimation = trigger('slideInAnimation', [
	state(
		'1',
		style({
			'marginLeft': '0'
		})
	),
	state(
		'0',
		style({
			'marginLeft': '-355px'
		})
	),
	state(
		'void',
		style({
			'marginLeft': '-355px'
		})
	),
	transition('1 => *', [
		animate(
			'0.2s 0s ease-in',
			style({
				'marginLeft': '-355px'
			})
		)
	]),
	transition('* => 1', [
		style({
			left: '-355px'
		}),
		animate('0.2s 0.2s ease-in')
	])
]);