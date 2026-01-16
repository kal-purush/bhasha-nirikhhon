import capitalize from 'lodash/capitalize'
import { getValidArray } from 'utils/common'

import { EOrderStatusEnum } from 'enums/order'
import { IOption } from 'interfaces/common'
import router from 'next/router'
import routes from 'routes'

export function mapAuthor(author: string): string[] {
  return getValidArray(author.split(','))
}

export function redirect(): void {
  router.push(`${routes.cms.orderManagement.value}?index=0&page=${router.query.page}&pageSize=${router.query.pageSize}`)
}

export const rentLengthOptions: number[] = Array.from({ length: 12 }, (_, i) => i + 1)

export function getOptionsSelect(): IOption[] {
  return Object.keys(EOrderStatusEnum).map((key) => ({
    label: capitalize(EOrderStatusEnum[key as keyof typeof EOrderStatusEnum]),
    value: EOrderStatusEnum[key as keyof typeof EOrderStatusEnum]
  }))
}

export function getRentLengthOptionsSelect(): IOption[] {
  return rentLengthOptions.map((rentLength: number) => ({
    label: `${rentLength} ${rentLength > 1 ? 'months' : 'month'}`,
    value: String(rentLength)
  }))
}