import { formatSiteMetadata } from 'graphql/formatters/formatSiteMetadata'
import {
  GetSiteMetadataDocument,
  type GetSiteMetadataQuery,
  type GetSiteMetadataQueryVariables,
} from 'graphql/generated/graphql'
import { client } from 'graphql/gqlClient'

export const getSiteMetadata = async () => {
  try {
    const { data, error } = await client.query<
      GetSiteMetadataQuery,
      GetSiteMetadataQueryVariables
    >(GetSiteMetadataDocument, {})

    return {
      metadata: formatSiteMetadata(data?.general?.siteMetadata),
      error,
    }
  } catch (error) {
    if (error instanceof Error) console.log(error.message)
    return { metadata: null, error }
  }
}