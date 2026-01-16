import { CookieConsentReactProps } from 'hds-react';

import siteSettings from '../../public/assets/siteSettings.json';
import { PAGE_HEADER_ID } from '../constants';

import useLocale from './useLocale';

const useCookieConsentSettings = () => {
  const locale = useLocale();

  const cookieConsentProps: CookieConsentReactProps = {
    onChange: () => {},
    siteSettings,
    options: { focusTargetSelector: `#${PAGE_HEADER_ID}`, language: locale },
  };

  return cookieConsentProps;
};

export default useCookieConsentSettings;