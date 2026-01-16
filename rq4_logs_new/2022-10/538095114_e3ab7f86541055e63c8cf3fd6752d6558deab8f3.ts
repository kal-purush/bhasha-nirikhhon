import { NAVIGATION_ANCHOR, type INavigationOption } from '$routes/types';
import { EToken } from '$l18n/enums';

const navOptions: INavigationOption[] = [
  {
    token: EToken.SERVICES,
    anchor: NAVIGATION_ANCHOR[EToken.SERVICES]
  },
  {
    token: EToken.ABOUT,
    anchor: NAVIGATION_ANCHOR[EToken.ABOUT]
  },
  {
    token: EToken.CONTACT,
    anchor: NAVIGATION_ANCHOR[EToken.CONTACT]
  }
];

export default navOptions;