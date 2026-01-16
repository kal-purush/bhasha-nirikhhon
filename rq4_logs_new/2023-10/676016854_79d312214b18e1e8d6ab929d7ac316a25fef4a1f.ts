import got from 'got'

async function isUrlReachable(url: string | null): Promise<boolean> {
  if (!url) {
    return false
  }

  try {
    await got.head(url)
    return true
  } catch (err) {
    return false
  }
}

export async function getCompatibleImageUrl(
  url: string | null,
  fallbackUrl: string | null
): Promise<string | null> {
  const image = (await isUrlReachable(url)) ? url : fallbackUrl

  if (image) {
    const imageUrl = new URL(image)

    if (imageUrl.host === 'images.unsplash.com') {
      if (!imageUrl.searchParams.has('w')) {
        imageUrl.searchParams.set('w', '1200')
        imageUrl.searchParams.set('fit', 'max')
        return imageUrl.toString()
      }
    }
  }

  return image
}