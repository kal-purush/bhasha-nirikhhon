import 'styled-components';

declare module 'styled-components' {
  export interface DefaultTheme {
    colors: {
      background: string;
      text: string;
      highlight: string;
      line: string;
    };
    breakpoints: {
      small: string;
      medium: string;
    };
  }
}