import { DefaultTheme } from 'styled-components'

export const DarkTheme: DefaultTheme = {
	bg: {
		primary: '#264653' /* japanese indigo */,
		secondary: '#2a9d8f' /* jungle green */,
	},
	text: {
		primary: '#ffffff' /* white */,
		secondary: '#e9c46a' /* hansa yellow */,
		link: '#f4a261' /* light red ochre */,
	},
}

// TODO: Define light theme color
export const LightTheme: DefaultTheme = {
	bg: {
		primary: '#ffffff',
		secondary: '#ffffff',
	},
	text: {
		primary: '#000000',
		secondary: '#000000',
		link: '#000000',
	},
}