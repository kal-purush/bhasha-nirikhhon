import StoryblokClient from 'storyblok-js-client'

export default defineEventHandler((event) => {
  const storyblok = new StoryblokClient({
    accessToken: process.env.STORYBLOK_TOKEN,
  })

  return storyblok.get('cdn/stories', {
    version: 'draft',
    starts_with: 'posts/',
    sort_by: 'created_at:desc',
  })
})