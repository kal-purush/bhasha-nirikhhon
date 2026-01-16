import HomeView from "../views/HomeView.vue";
import Login from "../views/Login/index.vue";
import NotFound from "../views/NotFound/index.vue";

export default [
  {
    path: "/",
    name: "home",
    component: HomeView,
  },
  {
    path: "/login",
    name: "login",
    component: Login,
  },
  {
    path: "/not-found",
    name: "notFound",
    component: NotFound,
  },
];