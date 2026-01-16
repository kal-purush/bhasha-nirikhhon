import { MenuItem } from '@onr/core';
import { findCurrentMenuKeys } from '../findCurrentMenuKeys';
import { getMenuItemKey } from '../getMenuItemKey';

describe('findCurrentMenuKeys test', () => {
  it('should return correct menu item', async () => {
    const menuItems: MenuItem[] = [
      {
        name: 'Todo List',
        path: '/todos',
        login: false,
      },
      {
        name: 'New Todos',
        login: false,
        children: [
          {
            name: 'Todo 1',
            path: '/todos/1',
            login: false,
          },
          {
            name: 'Todo 2',
            path: '/todos/2',
            login: false,
          },
        ],
      },
      {
        name: 'Recent Todos',
        path: '/todo',
        login: false,
        children: [
          {
            name: 'Todo 3',
            path: '/todos/3',
            login: false,
          },
          {
            name: 'Todo 4',
            path: '/todos/4',
            login: false,
          },
        ],
      },
    ];

    const { openKeys, selectedKeys } = findCurrentMenuKeys({
      pathname: '/todos/3',
      menuItems,
      openKeys: [],
    });

    const menuItem1 = menuItems[2];
    const menuItem2 = menuItems[2].children?.[0] as MenuItem;
    const key1 = getMenuItemKey(menuItem1.name, 2);
    const key2 = getMenuItemKey(menuItem2.name, 0, menuItem1.name);

    expect(openKeys).toEqual([key1, key2]);
    expect(selectedKeys).toEqual([key2]);
  });

  it('should return correct menu item 2', async () => {
    const menuItems: MenuItem[] = [
      {
        name: 'item1',
        login: false,
        children: [
          {
            name: 'item1-1',
            path: '/item/1-1',
            login: false,
          },
          {
            name: 'item1-2',
            login: false,
            children: [
              {
                name: 'item1-2-1',
                path: '/item/1-2-1',
                login: false,
              },
              {
                name: 'item1-2-2',
                login: false,
                children: [
                  {
                    name: 'item1-2-2-1',
                    path: '/item/1-2-2-1',
                    login: false,
                  },
                ],
              },
            ],
          },
        ],
      },
    ];

    const { openKeys, selectedKeys } = findCurrentMenuKeys({
      pathname: '/item/1-2-2-1',
      menuItems,
      openKeys: [],
    });

    const menuItem1 = menuItems[0];
    const menuItem2 = menuItems[0].children?.[1] as MenuItem;
    const menuItem3 = menuItems[0].children?.[1].children?.[1] as MenuItem;
    const menuItem4 = menuItems[0].children?.[1].children?.[1].children?.[0] as MenuItem;
    const key1 = getMenuItemKey(menuItem1.name, 0);
    const key2 = getMenuItemKey(menuItem2.name, 1, menuItem1.name);
    const key3 = getMenuItemKey(menuItem3.name, 1, menuItem2.name);
    const key4 = getMenuItemKey(menuItem4.name, 0, menuItem3.name);

    expect(openKeys).toEqual([key1, key2, key3, key4]);
    expect(selectedKeys).toEqual([key4]);
  });
});