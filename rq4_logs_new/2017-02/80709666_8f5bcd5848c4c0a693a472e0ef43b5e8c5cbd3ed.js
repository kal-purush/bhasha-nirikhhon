import Home from './pages/home.vue'
import Driver from './pages/driver.vue'

const routes = [
  {
    path: '/',
    name: 'Home',
    component: Home
  },
  {
    path: '/driver/:id',
    name: 'driver',
    component: Driver
  }
]

export default routes