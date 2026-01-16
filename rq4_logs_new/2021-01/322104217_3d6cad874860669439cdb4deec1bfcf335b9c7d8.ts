import styled from 'styled-components'

export const Wrapper = styled.div`
	display: inline-flex;
	align-items: center;
`

export const LeftLabel = styled.span`
	cursor: pointer;
	margin-right: 0.5em;
`

export const RightLabel = styled.span`
	cursor: pointer;
	margin-left: 0.5em;
`

export const Switch = styled.label`
	position: relative;
	display: inline-flex;
	width: 60px;
	height: 34px;
`

export const Checkbox = styled.input.attrs((props) => ({
	type: 'checkbox',
	checked: props.checked,
	onChange: props.onChange,
}))`
	opacity: 0;
	width: 0;
	height: 0;

	&:checked + .slider {
		background-color: ${({ theme }) => theme.bg.secondary};
	}

	&:focus + .slider {
		box-shadow: 0 0 1px ${({ theme }) => theme.bg.secondary};
	}

	&:checked + .slider:before {
		-webkit-transform: translateX(26px);
		-ms-transform: translateX(26px);
		transform: translateX(26px);
	}
`

export const Slider = styled.span.attrs({ className: 'slider' })`
	position: absolute;
	cursor: pointer;
	top: 0;
	left: 0;
	right: 0;
	bottom: 0;
	background-color: #ccc;
	-webkit-transition: 0.4s;
	transition: 0.4s;

	&:before {
		position: absolute;
		content: '';
		height: 26px;
		width: 26px;
		left: 4px;
		bottom: 4px;
		background-color: white;
		-webkit-transition: 0.4s;
		transition: 0.4s;
	}
`

export const RoundSlider = styled(Slider)`
	border-radius: 34px;

	&:before {
		border-radius: 50%;
	}
`