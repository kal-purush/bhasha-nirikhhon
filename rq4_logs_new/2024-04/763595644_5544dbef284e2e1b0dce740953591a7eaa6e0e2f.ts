import type { MenuItemOptions } from '../useAuth';
import type { NavItems, UserSettings } from './useSystemMenuItems.types';

export const NAV_ITEMS: NavItems = {
  gallery: {
    icon: 'faLightbulb',
  },
  pages: {
    auth: true,
    icon: 'faNewspaper',
  },
  themes: {
    auth: true,
    icon: 'faPalette',
  },
  tutorial: {
    icon: 'faBookOpen',
  },
  websites: {
    auth: true,
    icon: 'faGlobe',
  },
  widgets: {
    auth: true,
    icon: 'faPuzzlePiece',
  },
};

export const USER_SETTINGS: UserSettings = [
  {
    id: 'profile',
    auth: true,
    icon: 'faUser',
  },
  {
    id: 'settings',
    auth: false,
    icon: 'faGear',
  },
  {
    id: 'analytics',
    auth: true,
    icon: 'faChartLine',
  },
];

export const USER_MENU_ITEMS: MenuItemOptions<{ auth?: boolean }>[] = [
  ...USER_SETTINGS.map(({ id, icon, auth }) => ({
    auth,
    icon,
    label: `lbl-${id}`,
    href: `/user-settings#${id}`,
  })),
  'divider',
];