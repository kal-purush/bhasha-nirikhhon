/* eslint-disable react-hooks/rules-of-hooks */
import { useQuery } from '@tanstack/react-query'
import { ContactsListType } from '../../interfaces'
import eventOrganizationAPI from './event_organization.api'

class GetAllContactsByOrganizationIdQuery {
  private _query

  constructor(organizationId: string) {
    this._query = useQuery({
      queryKey: ['contacts', organizationId],
      queryFn: () => eventOrganizationAPI.getListContactsByOrganizationId(organizationId),
      enabled: organizationId !== '',
      staleTime: 5 * 60 * 1000
    })
  }

  fetch() {
    return this._query.data?.data as ContactsListType
  }
}

export { GetAllContactsByOrganizationIdQuery }