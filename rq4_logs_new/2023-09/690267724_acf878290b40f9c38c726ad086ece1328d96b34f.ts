// fetchingMethods.ts
import { getTableByName } from './services/databaseApi/controller'
import { mutations } from './store/columnStore'

export function addDataToLocalStorage(key: string, value: any) {
  return window.localStorage.setItem(key, JSON.stringify(value))
}

export function loadDataFromLocalStorage(key: string) {
  return window.localStorage.getItem(key)
}

export function localFetch() {
  const counterData: any = loadDataFromLocalStorage('counter')
  const columnsData: any = loadDataFromLocalStorage('columns')

  const counter = JSON.parse(counterData)
  const columns = JSON.parse(columnsData)

  if (counter) {
    mutations.setCounter(counter)
  }

  if (!columns) {
    mutations.setError('Cannot fetch data from local storage')
    return
  }

  const columnsKeys: string[] = Object.keys(columns)

  columnsKeys.map((key: string) => {
    mutations.addMultipleData(key, columns[key])
  })
}

export async function restApiFetch(callback: Function) {
  try {
    const config = { columns: ['A', 'B', 'C'], counter: 'counter' }

    const counter: { id?: number; value?: number }[] = await getTableByName(config.counter)
    const columns = await Promise.all(
      config.columns.map((columnName) => getTableByName(columnName))
    )

    if (counter.length > 0) {
      mutations.setCounter(Number(counter[0].value))
    }

    // mutations.setCounter(Number(counter))

    columns.reduce((acc: any, value: any, index: number) => {
      mutations.addMultipleData(config.columns[index], value)
    }, {})
  } catch (err: any) {
    // if (err.code === 'ERR_NETWORK') changeFetchMethod('fetchMethod', true, true)
    if (err.code === 'ERR_NETWORK') callback()
    mutations.setError(err)
  }
}

export function getFetchMode(fetchMethod: any, callback: Function) {
  const isFetching: string | null = loadDataFromLocalStorage('fetchMethod')
  if (!isFetching) return
  const isLocalFetching = JSON.parse(isFetching)
  //   changeFetchMethod('fetchMethod', isLocalFetching)
  callback(isLocalFetching)
  fetchMethod.value = isLocalFetching
}