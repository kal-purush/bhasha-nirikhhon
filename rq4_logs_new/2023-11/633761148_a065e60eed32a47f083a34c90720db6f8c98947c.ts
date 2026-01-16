import { cacheExchange as exchange } from '@urql/exchange-graphcache'
import introspectedSchema from './generated/introspection.json'

/*
 * These objects do not have an id.
 * so it is difficult to see if it is cachable.
 * It is also possible to create a cachable key from the response.
 * https://formidable.com/open-source/urql/docs/graphcache/normalized-caching
 */

export const cacheExchange = exchange({
  schema: introspectedSchema,
  keys: {
    LocationRecord: () => null,
    VideoField: () => null,
    CollectionMetadata: () => null,
    ConcertRecord: () => null,
    Tag: () => null,
    LatLonField: () => null,
    TwoColumnModelRightContentField: () => null,
    TwoColumnModelLeftContentField: () => null,
    TextBlockModelContentField: () => null,
    HeaderModelBodyField: () => null,
  },
})