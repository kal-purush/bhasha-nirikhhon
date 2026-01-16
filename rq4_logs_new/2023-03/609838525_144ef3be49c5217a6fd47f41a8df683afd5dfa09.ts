interface SitemapType {
  title: string;
  links: {
    label: string;
    url: string;
  }[];
}

const sitemap: SitemapType[] = [
  {
    title: 'Features',
    links: [
      {
        label: 'Link Shortening',
        url: '/',
      },
      {
        label: 'Branded Links',
        url: '/',
      },
      {
        label: 'Analytics',
        url: '/',
      },
    ],
  },
  {
    title: 'Resources',
    links: [
      {
        label: 'Blog',
        url: '/',
      },
      {
        label: 'Developers',
        url: '/',
      },
      {
        label: 'Support',
        url: '/',
      },
    ],
  },
  {
    title: 'Company',
    links: [
      {
        label: 'About',
        url: '/',
      },
      {
        label: 'Our Team',
        url: '/',
      },
      {
        label: 'Careers',
        url: '/',
      },
      {
        label: 'Contact',
        url: '/',
      },
    ],
  },
];

export default sitemap;