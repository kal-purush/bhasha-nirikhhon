import { createGlobalStyle, DefaultTheme } from 'styled-components';

declare module 'styled-components' {
  export interface DefaultTheme {
    colors: {
      primary: string;
      secondary: string;
      bg: string;
      text: string;
      buttonHover: string;
    };
  }
}

export const theme = {
  colors: {
    primary: '#4ECDC4',
    secondary: '#FF6B6B',
    bg: '#f7f7f7',
    text: '#333',
    buttonHover: '#FF6B6B',
  },
};

export const GlobalStyle = createGlobalStyle`
  * {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
  }

  body {
    font-family: 'Inter', sans-serif;
    background-color: ${({ theme }) => theme.colors.bg};
    color: ${({ theme }) => theme.colors.text};
  }

  button {
    cursor: pointer;
  }
`;