/* eslint-disable import/no-anonymous-default-export */
export default {
  title: "Next Movies",
  titleTemplate: "Next SEO | %s",
  description: "Database of movies created with Next.js and Strapi",
  openGraph: {
    type: 'website',
    locale: 'en_IE',
    url: 'https://www.nextmovies.test',
    site_name: 'Next Movies',
  },
  twitter: {
    handle: '@ivan_doric',
    site: '@WatchLearnTuts',
    cardType: 'summary_large_image',
  },
  additionalLinkTags:
    [
      {
        rel: 'icon',
        href: '/favicon.ico',
      },
      {
        rel: 'apple-touch-icon',
        href: '/vercel.svg',
        sizes: '76x76'
      },
      // {
      //   rel: 'manifest',
      //   href: '/manifest.json'
      // }
    ]
};