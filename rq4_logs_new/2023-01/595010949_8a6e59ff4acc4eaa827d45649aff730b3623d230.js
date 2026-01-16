import { createGlobalStyle } from 'styled-components'

export const GlobalStyle = createGlobalStyle`
* {
    box-sizing: border-box;
}

body {
    background-color: ${props => props.theme.colors.primary};
} 

`