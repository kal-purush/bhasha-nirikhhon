import * as LightWeightCharts from 'lightweight-charts'

export default defineNuxtPlugin((nuxtApp) => {
  // now available on `nuxtApp.$injected`
  // You can alternatively use this format, which comes with automatic type support
  return {
    simon: 'hello',
    provide: {
      lightweightCharts: LightWeightCharts
    }
  }
})