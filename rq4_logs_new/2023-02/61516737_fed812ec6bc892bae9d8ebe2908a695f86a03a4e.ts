import { get, readable, type Readable } from 'svelte/store';
import type { WebsiteEntry } from '../types';

type ImagetoolsResult = { default: string[] };
type GlobUrlStore = Readable<Record<string, ImagetoolsResult>>;

// Resize the website images at build time so that the appropriate (smaller)
// versions of each website image can be served
export const resizedWebsiteUrlMap: GlobUrlStore = readable(
  import.meta.glob('../../images/websites/*.jpg', {
    // Generate additional sizes for each pregenerated website image
    query: { format: 'jpg', width: '256;512' },
    // Resolve each import promise and store the final values
    eager: true
  })
);

// Retrieve the URL to the regular-sized thumbnail for this website
export function getWebsite1xThumbnailUrl(website: WebsiteEntry): string {
  const $resizedWebsiteUrlMap = get(resizedWebsiteUrlMap);
  const imagePath = `../../images/websites/${website.id}.jpg`;
  return $resizedWebsiteUrlMap[imagePath].default[0];
}

// Retrieve the URL to the high-density (Retina) thumbnail for this website
export function getWebsite2xThumbnailUrl(website: WebsiteEntry): string {
  const $resizedWebsiteUrlMap = get(resizedWebsiteUrlMap);
  const imagePath = `../../images/websites/${website.id}.jpg`;
  return $resizedWebsiteUrlMap[imagePath].default[1];
}