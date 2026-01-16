import { gql } from '@urql/core'
import { image } from './image'
import { concertLink } from './concertLink'
import { pageLink } from './pageLink'
import { eventBlock } from './eventBlock'
import { video } from './video'

export const headerBody = gql`
  fragment headerBody on HeaderModelBodyField {
    value
    links {
      ... on ConcertRecord {
        ...concertLink
      }
      ... on PageRecord {
        ...pageLink
      }
    }
    blocks
  }
  ${concertLink}
  ${pageLink}
  ${eventBlock}
  ${image}
  ${video}
`