import { Menu } from '@repo/ui/utils/menu/interface';

const navbar: Menu = {
  key: 'navbar',
  items: [
    {
      key: 'about',
      href: '/about',
      label: 'Sobre',
    },
    {
      key: 'contact',
      href: '/contact',
      label: 'Fale Conosco',
    },
  ],
};

const sidebar: Menu = {
  key: 'sidebar',
  items: [
    {
      key: 'profile',
      icon: 'user',
      href: '/profile',
      label: 'Meus Dados',
    },
  ],
};

export const menu: Array<Menu> = [navbar, sidebar];

export const logout: Menu['items'][number] = {
  key: 'logout',
  icon: 'exit',
  label: 'logout',
};