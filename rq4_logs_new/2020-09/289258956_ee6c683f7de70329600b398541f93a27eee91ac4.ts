
import 'styled-components'

type StyledComponentsTheme = {

  colors?: {
    mainBkg?: string;
    primary?: string;
    secondary?: string;
    accent?: string;
    deactive?: string;
    l1Container?: string;
    l2Container?: string;
    profit?: string;
    loss?: string;
    zeroNet?: string;
  },
  typographs?: {
    "main-title": string;
    head1: string;
    head2: string;
    head3: string;
    "primary-text": string;
    "secondary-text": string;
  }
}

declare module "styled-components" {
    interface DefaultTheme extends StyledComponentsTheme {}
}