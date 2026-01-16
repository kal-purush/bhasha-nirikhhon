import { getToken } from 'utils'

import { useAppSelector } from './redux'

export const useIsAuth = () => {
  const { user } = useAppSelector((state) => state.user)

  return !!getToken && !!user
}