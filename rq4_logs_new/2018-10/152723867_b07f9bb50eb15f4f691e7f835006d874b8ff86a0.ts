import Home from './Home/components/Home'
import About from './About/components/About'
import Counter from './Counter/components/Counter'
import NotFound from './components/NotFound'

const routes = [
  {
    path: '/',
    exact: true,
    component: Home
  },
  {
    path: '/about',
    component: About
  },
  {
    path: '/counter',
    component: Counter
  },
  {
    component: NotFound
  }
]

export default routes