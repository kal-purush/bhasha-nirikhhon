import { IconType } from 'react-icons';
import { RiHomeLine, RiUserLine, RxDashboard } from '@/utils';

export interface MenuLink {
  href       : string;
  title      : string;
  description: string;
  icon       : IconType;
}

export const MenuLinks: MenuLink[] = [
  {
    href: '/',
    title: 'Inicio',
    description: 'Ir a Inicio',
    icon: RiHomeLine,
  },
  {
    href: '/projects',
    title: 'Proyectos',
    description: 'Explora mis proyectos',
    icon: RxDashboard,
  },
  {
    href: '/about',
    title: 'Sobre Mí',
    description: 'Conoce más sobre mí',
    icon: RiUserLine,
  },
];