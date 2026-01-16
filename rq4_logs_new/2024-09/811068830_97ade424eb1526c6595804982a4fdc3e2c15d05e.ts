import 'styled-components';

// Theme type based the theme structure
declare module 'styled-components' {
  export interface DefaultTheme {
    disabledBackground: string;
    accentBackgroundColor: string;
    disabledText: string;
    textGrey: string;
    darkGrey: string;
    backgroundColor: string;
    blue: string;
    green: string;
    yellow: string;
    red: string;
    white: string;
  }
}