import type { IconCode } from '@weavcraft/core';
import type { UrlObject } from 'url';

export type Href = string | UrlObject;

export type MenuItemOptions<P = {}> = P &
  (
    | 'divider'
    | {
        href?: Href;
        icon?: IconCode;
        label: string;
        items?: (null | false | undefined | MenuItemOptions<P>)[];
      }
  );

export interface NavItem
  extends Exclude<MenuItemOptions<{ auth?: true }>, 'divider'> {
  href: string;
}

export type NavItems = Record<string, Pick<NavItem, 'auth' | 'icon'>>;

export const SIGNIN_METHODS = ['google'] as const;
export type SigninMethod = (typeof SIGNIN_METHODS)[number];
export type UserSettingId = 'profile' | 'settings' | 'analytics';

export type UserSettings = {
  auth: boolean;
  id: UserSettingId;
  icon: IconCode;
}[];