import { INavItem } from '@spwntch/react-ui';

export const NAV: INavItem[] = [
  // { label: 'Home', href: '/' },
  {
    label: 'About',
    href: '/about',
  },
  {
    label: 'Products',
    children: [
      {
        label: 'ICP Generator',
        description:
          'Create precise customer profiles instantly with our custom AI models.',
        href: '/products/icp-generator',
      },
      {
        label: 'Brand Strat Generator',
        description:
          'Create a detailed brand strategy instantly with our custom AI models.',
        href: '/products/bx-generator',
      },
      {
        label: 'Journey Map Generator',
        description:
          'Create rich customer journey maps instantly with our custom AI models.',
        href: '/products/journey-generator',
      },
    ],
  },
  // {
  //   label: 'Services',
  //   children: [
  //     {
  //       label: 'ICP Exploration',
  //       description:
  //         'Dive really deep into the mind of your ideal customer profile. Understand their needs, wants, and desires.',
  //       href: '/services/icp-exploration',
  //     },
  //     {
  //       label: 'Go-To-Market Strategy',
  //       description:
  //         'Develop a comprehensive go-to-market strategy that aligns with your business goals.',
  //       href: '/services/gtm-strategy',
  //     },
  //     {
  //       label: 'UX Deep Dive',
  //       description:
  //         'Get a comprehensive assessment of your user experience to identify areas of improvement.',
  //       href: '/services/ux-deep-dive',
  //     },
  //   ],
  // },
  { label: 'Blog', href: '/blog' },
  { label: 'Contact', href: '/contact' },
  // {
  //   label: 'Free Resources',
  //   href: '#resources',
  // },
  // { label: 'Get Started', href: '#get-started' },
];