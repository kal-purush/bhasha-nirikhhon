import type { MetadataRoute } from 'next';

export default async function sitemap(): Promise<MetadataRoute.Sitemap> {
  return [
    {
      url: 'https://www.saeternus.com',
      lastModified: new Date(),
      priority: 1,
    },
    {
      url: 'https://www.saeternus.com/courses/',
      lastModified: new Date(),
      priority: 0.8,
    },
    {
      url: 'https://www.saeternus.com/contact/',
      lastModified: new Date(),
      priority: 0.8,
    },
    {
      url: 'https://www.saeternus.com/about/',
      lastModified: new Date(),
      priority: 0.8,
    },
  ];
}