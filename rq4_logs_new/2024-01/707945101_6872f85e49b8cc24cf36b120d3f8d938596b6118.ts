import { styled } from 'styled-system/jsx'
import { hstack, square, stack } from 'styled-system/patterns'

const Root = styled('div', {
	base: stack.raw({
		direction: 'column',
		gap: '4',
		position: 'relative',
		column: 'content',
		sm: {
			direction: 'row',
		},
	}),
})

const LabelContainer = styled('div', {
	base: hstack.raw({ justify: 'space-between', lineHeight: '1' }),
})

const Control = styled('div', { base: { flexGrow: '1' } })

const Track = styled('div', {
	base: { h: '2', bg: 'fg.moreFaded', _osDark: { bg: 'fg.moreFaded' } },
})

const Range = styled('div', {
	base: { h: '2', bg: 'fg.muted' },
})

const Thumb = styled('div', {
	base: square.raw({
		size: '6',
		position: 'absolute',
		top: 'calc(-8 / 16 * 1rem)',
		cursor: 'pointer',
		bg: 'bg.canvas',
		borderWidth: '2px',
		borderColor: 'fg.muted',
		transition: 'colors',
		transitionDuration: 'faster',
		transitionTimingFunction: 'ease-out',
		_osDark: {
			borderWidth: '1.5px',
		},
	}),
})

const TickMarker = styled('span', {
	base: {
		mt: '-1',
		h: '2px',
		w: '2px',
		color: 'bg.canvas',
		bg: 'bg.canvas',
		transform: 'translateY(-50%)',
	},
})

const TextMarker = styled('span', {
	base: {
		mt: '3',
		color: 'fg.subtle',
		fontSize: '2xs',
		lineHeight: '1',
	},
})

export const Slider = {
	Root,
	LabelContainer,
	Control,
	Track,
	Range,
	Thumb,
	TickMarker,
	TextMarker,
}