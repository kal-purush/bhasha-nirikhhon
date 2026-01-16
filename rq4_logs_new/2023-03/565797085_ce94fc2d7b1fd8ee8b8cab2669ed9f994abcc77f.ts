import { RouteProps } from 'react-router-dom'
import { LOGIN_ROUTE, SIGNUP_ROUTE } from '../features/authorization/routes'
import { USER_ACCOUNT_ROUTE, USER_FAVORITE_ROUTE, USER_PROFILE_ROUTE } from '../features/users/routes'
import { CREATE_CHARACTER_ROUTE, MAIN_CHARACTER_ROUTE, SINGLE_CHARACTER_ROUTE } from '../features/characters/routes'
import { CREATE_EPISODE_ROUTE, MAIN_EPISODE_ROUTE, SINGLE_EPISODE_ROUTE } from '../features/episodes/routes'
import { CREATE_LOCATION_ROUTE, MAIN_LOCATION_ROUTE, SINGLE_LOCATION_ROUTE } from '../features/locations/routes'

export const PROTECTED_ROUTES: RouteProps[] = [
  /* Authorization */
  SIGNUP_ROUTE,
  LOGIN_ROUTE,

  /* Users */
  USER_ACCOUNT_ROUTE,
  USER_PROFILE_ROUTE,
  USER_FAVORITE_ROUTE,

  /* Characters */
  MAIN_CHARACTER_ROUTE,
  SINGLE_CHARACTER_ROUTE,
  CREATE_CHARACTER_ROUTE,

  /* Episodes */
  MAIN_EPISODE_ROUTE,
  SINGLE_EPISODE_ROUTE,
  CREATE_EPISODE_ROUTE,

  /* Locations */
  MAIN_LOCATION_ROUTE,
  SINGLE_LOCATION_ROUTE,
  CREATE_LOCATION_ROUTE

  /* Common */
]