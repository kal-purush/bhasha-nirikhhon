import 'styled-components/native'

// and extend them!
declare module 'styled-components' {
  export interface DefaultTheme {
    borderRadius: string
    colors: {
      primary: string
      primaryDark: string
    }
  }
}