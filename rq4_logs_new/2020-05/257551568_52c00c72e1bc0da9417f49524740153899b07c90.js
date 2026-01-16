import { fetchMenuLinks } from '../compositions/main-menu'
import { fetchConfiguration } from '../compositions/app-configuration'

export default function layoutData ({ store, $axios }) {
  return Promise.all([
    fetchMenuLinks(store),
    fetchConfiguration($axios)
  ])
}