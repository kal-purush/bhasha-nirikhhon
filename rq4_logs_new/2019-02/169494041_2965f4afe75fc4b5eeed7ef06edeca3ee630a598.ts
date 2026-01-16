import { ZtoRoutes } from '../models/zto-routes';
import { FirstUsePage } from '../pages/first-use/first-use';
import { WelcomePage } from '../pages/welcome/welcome';
import { SignInPage } from '../pages/sign-in/sign-in';
import { SignUpPage } from '../pages/sign-up/sign-up';
import { HomePage } from '../pages/home/home';
import { SettingsPage } from '../pages/settings/settings';
import { AboutPage } from '../pages/about/about';
import { TabsPage } from '../pages/tabs/tabs';

export const appRoutes: ZtoRoutes = {
  FIRST_USE: {
    id: 'FIRST_USE',
    page: FirstUsePage
  },
  WELCOME: {
    id: 'WELCOME',
    page: WelcomePage
  },
  SIGN_IN: {
    id: 'SIGN_IN',
    page: SignInPage
  },
  SIGN_UP: {
    id: 'SIGN_UP',
    page: SignUpPage
  },
  TABS: {
    id: 'TABS',
    page: TabsPage
  },
  HOME: {
    id: 'HOME',
    page: HomePage,
    contexts: {
      menu: {
        show: true,
        idx: 0,
        name: 'HOME',
        icon: 'home',
      },
      tabs: {
        show: true,
        idx: 0,
        name: 'HOME',
        icon: 'home',
      },
    }
  },
  SETTINGS: {
    id: 'SETTINGS',
    page: SettingsPage,
    contexts: {
      menu: {
        idx: 1,
        show: true,
        name: 'SETTINGS',
        icon: 'settings',
      },
      tabs: {
        idx: 1,
        show: false,
        name: 'SETTINGS',
        icon: 'settings',
      }
    }
  },
  ABOUT: {
    id: 'ABOUT',
    page: AboutPage,
    contexts: {
      menu: {
        show: true,
        idx: 2,
        name: 'ABOUT',
        icon: 'informations-circle',
      },
      tabs: {
        show: true,
        idx: 2,
        name: 'ABOUT',
        icon: 'informations-circle',
      },
    }
  },
};