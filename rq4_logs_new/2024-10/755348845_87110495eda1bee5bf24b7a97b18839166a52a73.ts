import { Booth } from "@/app/student/map/lib/booths"
import { Exhibitor } from "@/components/shared/hooks/api/useExhibitors"

// Filtering assumptions:
// - selecting multiple options for a filter gives the union (not intersection) of those options
// - selecting no options is the same as selecting all options
// - filters are ignored when using the search bar

export type FilterKey = "employments" | "industries"
export type FilterItem = Exhibitor[FilterKey][number]

export type Filter = {
  key: FilterKey
  items: FilterItem[]
  selected: FilterItem[]
  label: string
}
export type FilterMap = { [K in FilterKey]: Filter }

export function makeFilter(
  key: FilterKey,
  label: string,
  exhibitors: Exhibitor[]
): Filter {
  return {
    key,
    label,
    selected: [],
    items: getAllFilterOptions(key, exhibitors)
  }
}

export function filterBySearch<T extends Exhibitor | Booth>(
  items: T[],
  text: string
) {
  return items.filter(item =>
    getExhibitor(item).name.toLowerCase().includes(text.toLowerCase())
  )
}

export function applyFilters<T extends Exhibitor | Booth>(
  items: T[],
  filters: Filter[]
): T[] {
  return items.filter(e => filters.every(f => satisfiesFilter(e, f)))
}

function isBooth(item: Exhibitor | Booth): item is Booth {
  return (item as Booth).exhibitor !== undefined
}

function getExhibitor(item: Exhibitor | Booth): Exhibitor {
  return isBooth(item) ? item.exhibitor : item
}

function satisfiesFilter(item: Exhibitor | Booth, filter: Filter) {
  if (filter.selected.length === 0) return true

  return getExhibitor(item)[filter.key].some((x: { id: number }) =>
    filter.selected.some(y => x.id === y.id)
  )
}

// Gets all the possible options by looping over each exhibitor
function getAllFilterOptions(
  key: FilterKey,
  exhibitors: Exhibitor[]
): FilterItem[] {
  const distinct = new Map<FilterItem["id"], FilterItem>()
  exhibitors.forEach(e => {
    e[key].forEach(item => distinct.set(item.id, item))
  })
  return Array.from(distinct.values()).sort((a, b) =>
    a.name.localeCompare(b.name)
  )
}