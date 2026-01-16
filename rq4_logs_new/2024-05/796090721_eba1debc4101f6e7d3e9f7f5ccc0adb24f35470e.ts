export const About = () => import('@/views/AboutView.vue')

// Events related pages
export const EventView = () => import('@/views/event/EventView.vue')
export const EventDetail = () => import('@/views/event/EventDetail.vue')
export const EventRegister = () => import('@/views/event/EventRegister.vue')
export const EventEdit = () => import('@/views/event/EventEdit.vue')

// Error pages
export const NotFoundResource = () => import('@/views/NotFoundView.vue')
export const NetworkError = () => import('@/views/NetworkError.vue')