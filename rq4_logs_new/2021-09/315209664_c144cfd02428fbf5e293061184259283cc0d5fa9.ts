import { useQuery, useMutation } from '@apollo/react-hooks'
import gql from 'graphql-tag'
import { useApp } from '../contexts/AppContext'
import hasura from '../hasura'

export type AppPageProps = {
  id: string
  path: string | null
  title: string | null
  craftData: { [key: string]: string } | null
  editorId: string | null
  editorName: string | null
  publishedAt: Date | null
  updatedAt: Date | null
  options: { [key: string]: string } | null
}

export const useAppPage = (pageId: string) => {
  const { loading, error, data, refetch } = useQuery<hasura.GET_APP_PAGE, hasura.GET_APP_PAGEVariables>(
    gql`
      query GET_APP_PAGE($pageId: uuid!) {
        app_page_by_pk(id: $pageId) {
          id
          path
          title
          craft_data
          updated_at
          published_at
          options
          editor_id
          editor {
            name
          }
        }
      }
    `,
    {
      variables: {
        pageId,
      },
    },
  )

  const appPage: AppPageProps | null = data?.app_page_by_pk
    ? {
        id: data.app_page_by_pk.id,
        path: data.app_page_by_pk.path,
        title: data.app_page_by_pk.title,
        craftData: data.app_page_by_pk.craft_data,
        editorId: data.app_page_by_pk.editor_id,
        editorName: data.app_page_by_pk.editor?.name || null,
        publishedAt: data.app_page_by_pk.published_at ? new Date(data.app_page_by_pk.published_at) : null,
        updatedAt: data.app_page_by_pk.updated_at ? new Date(data.app_page_by_pk.updated_at) : null,
        options: data.app_page_by_pk.options,
      }
    : null

  return {
    refetchAppPage: refetch,
    loadingAppPage: loading,
    errorAppPage: error,
    appPage,
  }
}

export const useAppPageCollection = () => {
  const { id: appId } = useApp()
  const { loading, error, data, refetch } = useQuery<
    hasura.GET_APP_PAGE_COLLECTION,
    hasura.GET_APP_PAGE_COLLECTIONVariables
  >(
    gql`
      query GET_APP_PAGE_COLLECTION($appId: String!) {
        app_page(where: { app_id: { _eq: $appId }, is_deleted: { _eq: false } }) {
          id
          path
          title
          craft_data
          updated_at
          published_at
          options
          editor_id
          editor {
            name
          }
        }
      }
    `,
    {
      variables: {
        appId,
      },
    },
  )

  const appPages: AppPageProps[] =
    data?.app_page
      .map(v => ({
        id: v.id,
        path: v.path,
        title: v.title,
        craftData: v.craft_data,
        editorId: v.editor_id,
        editorName: v.editor?.name || null,
        publishedAt: v.published_at ? new Date(v.published_at) : null,
        updatedAt: v.updated_at ? new Date(v.updated_at) : null,
        options: v.options,
      }))
      .filter(v => v.craftData) || []

  return {
    refetchAppPages: refetch,
    loadingAppPages: loading,
    errorAppPages: error,
    appPages,
  }
}

export const useMutateAppPage = () => {
  const { id: appId } = useApp()
  const [insertAppPageHandler] = useMutation<hasura.INSERT_APP_PAGE, hasura.INSERT_APP_PAGEVariables>(gql`
    mutation INSERT_APP_PAGE($appId: String!, $path: String, $editorId: String!, $title: String!, $craftData: jsonb) {
      insert_app_page_one(
        object: { app_id: $appId, path: $path, title: $title, editor_id: $editorId, craft_data: $craftData }
      ) {
        id
      }
    }
  `)
  const insertAppPage = (values: {
    path?: string
    title: string
    editorId: string
    craftData: { [key: string]: any } | null
  }) => {
    return insertAppPageHandler({
      variables: { appId, ...values },
    })
  }
  const [updateAppPageHandler] = useMutation<hasura.UPDATE_APP_PAGE, hasura.UPDATE_APP_PAGEVariables>(gql`
    mutation UPDATE_APP_PAGE($pageId: uuid!, $updated: app_page_set_input) {
      update_app_page_by_pk(pk_columns: { id: $pageId }, _set: $updated) {
        id
      }
    }
  `)
  const updateAppPage = (values: {
    pageId: string
    path?: string
    title?: string
    editorId?: string
    publishedAt?: Date | null
    craftData?: { [key: string]: any }
    isDeleted?: boolean
  }) => {
    return updateAppPageHandler({
      variables: {
        pageId: values.pageId,
        updated: {
          ...(values.path ? { path: values.path } : {}),
          ...(values.title ? { title: values.title } : {}),
          ...(values.editorId ? { editor_id: values.editorId } : {}),
          ...(values.publishedAt !== undefined ? { published_at: values.publishedAt } : {}),
          ...(values.craftData ? { craft_data: values.craftData } : {}),
          is_deleted: values.isDeleted,
        },
      },
    })
  }

  return {
    insertAppPage,
    updateAppPage,
  }
}