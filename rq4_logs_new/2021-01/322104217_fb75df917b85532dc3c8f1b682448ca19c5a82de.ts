import styled from 'styled-components'
import { withAlphaHex } from 'with-alpha-hex'

export const ButtonIcon = styled.img`
	width: 3em;
	margin: 1em;
`

export const ButtonTitle = styled.span`
	font-size: 1.5em;
	text-transform: uppercase;
	margin: 0.2em 0.5em 0 0.5em;
`

export const ButtonSubtitle = styled.p`
	font-size: 0.7em;
`

export const StyledButton = styled.button`
	display: flex;
	align-items: center;
	justify-content: center;
	flex-direction: column;
	padding: 0.6em 1em;

	font-family: Verdana, Geneva, Tahoma, sans-serif;
	color: ${({ theme }) => theme.text.secondary};
	background-color: ${({ theme }) =>
		withAlphaHex(theme.bg.buttonBackground, 0.5)};
	cursor: pointer;

	border: 0.1em solid ${({ theme }) => withAlphaHex(theme.bg.secondary, 0.5)};
	border-radius: 0.8em;

	&:hover {
		color: ${({ theme }) => theme.bg.primary};
		background-color: ${({ theme }) =>
			withAlphaHex(theme.text.secondary, 0.7)};
	}
`