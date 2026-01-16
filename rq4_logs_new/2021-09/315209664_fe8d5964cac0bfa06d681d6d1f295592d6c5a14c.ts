import { useQuery } from '@apollo/react-hooks'
import gql from 'graphql-tag'
import hasura from '../hasura'
import { PodcastAlbum } from '../types/podcastAlbum'

export const usePodcastAlbumAdmin = (podcastAlbumId: string) => {
  const { loading, error, data, refetch } = useQuery<hasura.GET_PODCAST_ALBUM, hasura.GET_PODCAST_ALBUMVariables>(
    gql`
      query GET_PODCAST_ALBUM($podcastAlbumId: uuid!) {
        podcast_album_by_pk(id: $podcastAlbumId) {
          id
          title
          cover_url
          description
          is_public
          is_deleted
          published_at
          podcast_album_categories(order_by: { position: asc }) {
            category {
              id
              name
            }
          }
          podcast_album_podcast_programs {
            id
            position
            podcast_program {
              id
              title
              cover_url
              duration_second
            }
          }
          member {
            id
            name
          }
        }
      }
    `,
    { variables: { podcastAlbumId } },
  )
  const podcastAlbum: PodcastAlbum = {
    id: data?.podcast_album_by_pk?.id,
    title: data?.podcast_album_by_pk?.title || '',
    coverUrl: data?.podcast_album_by_pk?.cover_url || '',
    author: {
      id: data?.podcast_album_by_pk?.member.id || '',
      name: data?.podcast_album_by_pk?.member.name || '',
    },
    description: data?.podcast_album_by_pk?.description || '',
    isPublic: !!data?.podcast_album_by_pk?.is_public,
    isDeleted: !!data?.podcast_album_by_pk?.is_deleted,
    publishedAt: data?.podcast_album_by_pk?.published_at,
    podcastPrograms:
      data?.podcast_album_by_pk?.podcast_album_podcast_programs.map(v => ({
        id: v?.podcast_program?.id,
        podcastAlbumPodcastProgramId: v.id,
        title: v.podcast_program?.title || '',
        coverUrl: v.podcast_program?.cover_url || '',
        position: v.position,
        durationSecond: v.podcast_program?.duration_second || 0,
      })) || [],
    categories:
      data?.podcast_album_by_pk?.podcast_album_categories.map(v => ({
        id: v?.category?.id || '',
        name: v?.category?.name || '',
      })) || [],
  }

  return {
    loading,
    error,
    podcastAlbum,
    refetch,
  }
}

export const usePodcastAlbumCounts = (appId: string) => {
  const { loading, error, data, refetch } = useQuery<
    hasura.GET_PODCAST_ALBUM_COUNTS,
    hasura.GET_PODCAST_ALBUM_COUNTSVariables
  >(
    gql`
      query GET_PODCAST_ALBUM_COUNTS($appId: String!) {
        draft: podcast_album_aggregate(where: { app_id: { _eq: $appId }, published_at: { _is_null: true } }) {
          aggregate {
            count
          }
        }
        published: podcast_album_aggregate(where: { app_id: { _eq: $appId }, published_at: { _is_null: false } }) {
          aggregate {
            count
          }
        }
      }
    `,
    {
      variables: { appId },
    },
  )
  const counts: { [key: string]: number } | null = data
    ? {
        draft: data?.draft.aggregate?.count || 0,
        published: data?.published.aggregate?.count || 0,
      }
    : null

  return {
    loadingPodcastAlbumCounts: loading,
    errorPodcastAlbumCounts: error,
    counts,
    refetchPodcastAlbumCounts: refetch,
  }
}