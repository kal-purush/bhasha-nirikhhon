import styled from 'styled-components'

export const ButtonIcon = styled.img`
	width: 2.5em;
`

export const ButtonTitle = styled.span`
	font-size: 1em;
	text-transform: uppercase;
	margin: 0.2em 0.5em;
`

export const ButtonSubtitle = styled.p`
	font-size: 0.5em;
	margin: 0;
	padding: 0;
`

export const ButtonCenter = styled.div`
	display: flex;
	align-items: center;
	justify-content: center;
	flex-direction: column;
	font-family: Verdana, Geneva, Tahoma, sans-serif;
	padding: 0.6em 1em;
	cursor: pointer;
	border-radius: 0.8em;
	background-color: ${({ theme }) => theme.bg.secondary};
	color: ${({ theme }) => theme.text.secondary};
	&:hover {
		background-color: ${({ theme }) => theme.bg.secondary};
		color: ${({ theme }) => theme.text.link};
	}
`

export const ButtonLine = styled.div`
	display: flex;
	align-items: center;
	justify-content: center;
	font-family: Verdana, Geneva, Tahoma, sans-serif;
	padding: 0.6em 1em;
	cursor: pointer;
	border-radius: 0.8em;
	background-color: ${({ theme }) => theme.bg.secondary};
	color: ${({ theme }) => theme.text.secondary};
	&:hover {
		background-color: ${({ theme }) => theme.bg.secondary};
		color: ${({ theme }) => theme.text.link};
	}
`