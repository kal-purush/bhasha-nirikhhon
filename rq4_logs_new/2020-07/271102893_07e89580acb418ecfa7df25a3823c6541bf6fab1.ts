import { ServiceContext } from '@vtex/api'

import { getSettingsByCountry } from '../utils/getSettingsByCountry'

export function allCountrySettings(ctx: ServiceContext) {
  const allSettings = getSettingsByCountry(ctx.vtex.settings)

  ctx.body = Object.values(allSettings)
}