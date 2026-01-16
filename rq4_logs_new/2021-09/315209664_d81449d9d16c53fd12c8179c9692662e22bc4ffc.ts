import { useMutation, useQuery } from '@apollo/react-hooks'
import gql from 'graphql-tag'
import hasura from '../hasura'

export const useDefaultPermissions = () => {
  const { loading, error, data, refetch } = useQuery<hasura.GET_ROLE_PERMISSION>(gql`
    query GET_ROLE_PERMISSION {
      role_permission {
        id
        role_id
        permission_id
      }
    }
  `)

  const defaultRolePermissions =
    data?.role_permission.reduce<
      {
        [roleId in string]?: string[]
      }
    >(
      (accumulator, currentValue) => ({
        ...accumulator,
        [currentValue.role_id]: [...(accumulator[currentValue.role_id] || []), currentValue.permission_id],
      }),
      {},
    ) || {}

  return {
    loadingPermissions: loading,
    errorPermissions: error,
    defaultRolePermissions,
    refetchPermissions: refetch,
  }
}

export const usePermissionGroupsCollection = (appId: string) => {
  const { loading, error, data, refetch } = useQuery<
    hasura.GET_PERMISSION_GROUPS,
    hasura.GET_PERMISSION_GROUPSVariables
  >(
    gql`
      query GET_PERMISSION_GROUPS($appId: String) {
        permission_group(where: { app_id: { _eq: $appId } }) {
          id
          name
          permission_group_permissions {
            id
            permission_id
          }
        }
      }
    `,
    { variables: { appId } },
  )

  const permissionGroups: {
    id: string
    name: string
    permissionGroupPermissions: string[]
  }[] =
    data?.permission_group.map(v => ({
      id: v.id,
      name: v.name,
      permissionGroupPermissions: v.permission_group_permissions.map(w => w.permission_id) || [],
    })) || []

  return {
    loadingPermissionGroups: loading,
    errorPermissionGroups: error,
    permissionGroups,
    refetchPermissionGroups: refetch,
  }
}