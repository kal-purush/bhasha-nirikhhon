import { ref } from 'vue'

async function apiEndpointWarning() {
  console.error(`Provide api function reference as parameter on DataTableComposable in the setup()
  hook. Example: DataTableComposable(API.clients.index)`)
  return false
}

export default function DataTableComposable(apiEndpoint: Function = apiEndpointWarning) {
  const loading = ref(true)
  const  itemsLength = ref(0)
  const sortBy = ref([{
    key: 'id',
    order: 'asc'
  }])
  const items = ref([])

  const loadItems = (filters: any = undefined, search: string = '') => {
    loading.value = true
    apiEndpoint({ ...filters, search })
      .then((response: any) => {
        itemsLength.value = response.data.total
        items.value = response.data.items
      }).catch((err: any) => {
        console.warn(err)
      }).finally(() => {
        loading.value = false
      })
  }

  return {
    loading,
    itemsLength,
    sortBy,
    items,
    apiEndpoint,
    loadItems
  }
}