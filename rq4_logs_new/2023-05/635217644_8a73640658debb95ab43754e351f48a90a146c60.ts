import { RouteLocationNormalized, NavigationGuardNext } from 'vue-router'
import AuthService from '@/contracts/interfaces/AuthService'

export const requireAuth = (authService: AuthService) => async (to: RouteLocationNormalized, from: RouteLocationNormalized, next: NavigationGuardNext) => {
  const token: string | null = localStorage.getItem('token')
  if (!token && to.meta.requiresAuth) {
    next('/login')
    return
  }
  if (token) {
    const isAuthenticated: boolean = await authService.checkToken(token)
    if (!isAuthenticated) {
      next('/login')
    } else {
      next()
    }
  } else {
    next()
  }
}