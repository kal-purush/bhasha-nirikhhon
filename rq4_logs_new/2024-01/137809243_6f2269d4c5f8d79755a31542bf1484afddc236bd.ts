import {ContentAlternatePositionWithImage} from '@/components/ui/organisms/content-sections/content-alternate-position-with-image-item/content-alternate-position-with-image.model'
import {faker} from '@faker-js/faker'
import {imageFactory} from '@/tests/factories/image.factory'

export function contentAlternatePositionWithImageItemFactory(): ContentAlternatePositionWithImage {
  return {
    title: faker.lorem.sentence(),
    description: faker.lorem.sentence(),
    link: faker.internet.url(),
    image: imageFactory()
  }
}