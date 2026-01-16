import rss, { pagesGlobToRssItems } from '@astrojs/rss';

export async function get() {
  return rss({
    title: 'Ugmsn | Blog',
    description: 'My journey learning Astro',
    site: 'https://ugmsn-blog.netlify.app',
    items: await pagesGlobToRssItems(import.meta.glob('./**/*.md')),
    customData: `<language>en-uk</language>`,
  });
}