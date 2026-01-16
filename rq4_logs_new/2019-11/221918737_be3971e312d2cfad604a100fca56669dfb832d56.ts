import { createGlobalStyle } from 'styled-components';
import reboot from 'styled-reboot';
import { defaultTheme } from './defaultTheme';

const rebootOptions = {
  fontFamilyBase: defaultTheme.font.family.body,
  // TODO: Replace these with correct values when in theme
  // headingsMarginBottom: space[3],
  // paragraphMarginBottom: space[4],
  // labelMarginBottom: space[3],
  // lineHeightBase: font.lineHeights.display,
};

const GlobalStyle = createGlobalStyle`
  @import url('https://fonts.googleapis.com/css?family=Poppins|Open+Sans');
  ${reboot(rebootOptions)}
  h1, h2, h3, h4, h5, h6 {
    font-family: ${defaultTheme.font.family.title}
  }
`;

export { GlobalStyle };