import { createClient } from '@sanity/client';
import imageUrlBuilder from '@sanity/image-url';
const token = import.meta.env.SANITY_SECRET_TOKEN;
// const projectId = import.meta.env.REACT_APP_SANITY_PROJECT_ID;
export const client = createClient({
  projectId: 'qq832t9b',
  dataset: 'production',
  useCdn: true,
  // apiVersion: '2023-05-03',
  token: token,

  ignoreBrowserTokenWarning: false,
});

const builder = imageUrlBuilder(client);

export function urlFor(source: string) {
  return builder.image(source);
}