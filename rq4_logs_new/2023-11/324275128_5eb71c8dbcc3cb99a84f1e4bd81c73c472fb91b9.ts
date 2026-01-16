import { MenuItem } from '@onr/core';
import { getMenuItemKey } from './getMenuItemKey';

interface Props {
  pathname: string;
  menuItems: MenuItem[];
  openKeys: string[];
  parentMenu?: MenuItem;
}

export const findCurrentMenuKeys = (
  props: Props,
): {
  openKeys: string[];
  selectedKeys: string[];
} => {
  const { pathname, menuItems, openKeys, parentMenu } = props;
  // first layer search
  for (var i = 0; i < menuItems.length; i++) {
    if (menuItems[i].path === pathname) {
      const key = getMenuItemKey(menuItems[i].name, i, parentMenu?.name);
      openKeys.push(key);

      return {
        selectedKeys: [key],
        openKeys,
      };
    }
  }

  // nested layer search
  for (var i = 0; i < menuItems.length; i++) {
    const subMenuItems = menuItems[i].children;
    if (subMenuItems) {
      const key = getMenuItemKey(menuItems[i].name, i, parentMenu?.name);
      openKeys.push(key);

      return findCurrentMenuKeys({
        pathname,
        openKeys,
        menuItems: subMenuItems,
        parentMenu: menuItems[i],
      });
    }
  }

  return {
    selectedKeys: [],
    openKeys: [],
  };
};