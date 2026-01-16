/* eslint-disable react-hooks/rules-of-hooks */
import { useQuery } from '@tanstack/react-query'
import eventCategoryAPI from './event_category.api'
import { ActivitiesListType } from '../../interfaces'

class GetAllActivitiesByCategoryIdQuery {
  private _query

  constructor(categoryId: string) {
    this._query = useQuery({
      queryKey: ['activities', categoryId],
      queryFn: () => eventCategoryAPI.getListActivitiesByCategoryId(categoryId),
      enabled: categoryId !== ''
    })
  }

  fetch() {
    return this._query.data?.data as ActivitiesListType
  }
}

export { GetAllActivitiesByCategoryIdQuery }