import { createGlobalStyle } from 'styled-components';

export const GlobalStyle = createGlobalStyle`
  * {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
    outline: 0;
    text-decoration: none;
    -webkit-tap-highlight-color: transparent;
  }

  html {
    @media (max-width: 1080px){
      font-size: 93.75%; // 15px
    }
    @media (max-width: 720px){
      font-size: 81.25%; // 13px
    }
  }

    html {
    @media (max-width: 1080px){
      font-size: 93.75%; // 15px
    }
    @media (max-width: 720px){
      font-size: 81.25%; // 13px
    }
  }

  img {
    display: block;
    -webkit-user-drag: none;
    -khtml-user-drag: none;
    -moz-user-drag: none;
    -o-user-drag: none;
  }

  label {
    cursor: inherit;
  }

  button {
    border: 0;
  }

  html, body {
    width: 100%;
  }

  body {
    -webkit-font-smoothing: antialiased;
    overflow-x: hidden;
    -webkit-box-sizing: border-box;
    -moz-box-sizing: border-box;
    box-sizing: border-box;
  }

  body, input, button {
    font-size: 1rem;
  }

  button {
    cursor: pointer;
    background-color: transparent;
  }
`;