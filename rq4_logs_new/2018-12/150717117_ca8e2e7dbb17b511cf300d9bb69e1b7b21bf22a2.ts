import * as styledComponents from 'styled-components'
import { ThemedStyledComponentsModule } from 'styled-components'

const {
  default: styled,
  css,
  createGlobalStyle,
  keyframes,
  ThemeProvider
} = styledComponents as ThemedStyledComponentsModule<
  ThemeInterface
>

export interface ThemeInterface {
  black: string,
  grey: string,
  lightgrey: string,
  offWhite: string,
  maxWidth: string,
  bs: string,
  malachite: string,
  pigmentGreen: string,
  mintCream: string,
  whiteIce: string,
  mediumAquamarine: string,
  aeroBlue: string
}

export const theme = {
  black: '#393939',
  grey: '#3A3A3A',
  lightgrey: '#E1E1E1',
  offWhite: '#EDEDED',
  maxWidth: '1000px',
  bs: '0 12px 24px 0 rgba(0, 0, 0, 0.09)',
  malachite: '#01D25D',
  pigmentGreen: '#01B54A',
  mintCream: '#F5FBF8',
  whiteIce: '#D5F7E7',
  mediumAquamarine: '#57E49E',
  aeroBlue: '#CCE1D6'
}

export default styled

export { css, createGlobalStyle, keyframes, ThemeProvider }

/*
app {
    --mint-cream: #F5FBF8;
    --malachite: #01D25D;
    --white-ice: #D5F7E7;
    --pigment-green: #01B54A;
    --medium-aquamarine: #57E49E;
    --aero-blue: #CCE1D6;
}

cards {
    --alice-blue: #F3F9FC;
    --solitude: #E4E5E6;
    --orange-peel: #FFA005;
    --dodger-blue: #1E96F4;
    --malachite: #15D460;
    --alizarin: #FE303C;
}

folders {
  --alice-blue: #F4F9FC;
  --porcelain: #DEDCDA;
  --radical-red: #FF4453;
  --dodger-blue: #0A93FB;
  --golden-tainoi: #FFC851;
  --gum-leaf: #A1C1AC;
}

profile {
    --alice-blue: #F8FAFB;
    --medium-sea-green: #2DD264;
    --mystic: #D6DAD7;
    --pale-oyster: #9C9071;
    --casablanca: #F3A74E;
    --paco: #4E3C36;
}

store {
    --alice-blue: #F7FAFB;
    --gainsboro: #DEDEDE;
    --dodger-blue: #0598F6;
    --pattens-blue: #D5EDFA;
    --dark-cerulean: #00397A;
    --navy-blue: #1D7CD7;
}

*/