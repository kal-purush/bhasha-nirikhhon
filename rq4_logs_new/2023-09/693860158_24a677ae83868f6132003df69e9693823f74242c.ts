import 'styled-components'

declare module 'styled-components' {
  export interface DefaultTheme {
    colors: {
      body: string
      bodyPattern: string
      card: string
      text: string
      headings: string
      chart: string
      primary: {
        green: string
        red: string
      }
      social: {
        facebook: string
        twitter: string
        instagram: string
        youtube: string
      }
      toggle: {
        default: string
        active: string
      }
    }
  }
}