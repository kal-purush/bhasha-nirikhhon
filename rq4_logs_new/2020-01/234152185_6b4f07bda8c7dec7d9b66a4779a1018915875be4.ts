import { createGlobalStyle } from 'styled-components'
import reset from 'styled-reset'

export const GlobalStyle = createGlobalStyle`
  ${reset}
  /* other styles */
  @import url('https://fonts.googleapis.com/css?family=Maven+Pro&display=swap');
  *{
      box-sizing: border-box;
  }
  body{
      font-family: -apple-system,system-ui,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif
  }
  a{
      color: inherit;
      text-decoration: none
  }
  input,
  button{
      &:focus, &:active{outline:none}
  }
`