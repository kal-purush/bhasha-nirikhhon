// routes.ts

import Home from "./components/Home"
import Account from "./components/Account"
import AccountProfile from "./components/AccountProfile"
import AccountBilling from "./components/AccountBilling"
import AccountDelete from "./components/AccountDelete"
import Plugs from "./components/Plugs"
import PlugsNew from "./components/PlugsNew"
import PlugDetail from "./components/PlugDetail"
import PlugStatus from "./components/PlugStatus"
import PlugSettings from "./components/PlugSettings"

interface RouteConfig {
  path: string
  component: React.ComponentType
  children?: RouteConfig[]
  exact?: boolean
}

const routes: RouteConfig[] = [
  {
    path: "/",
    component: Home,
    exact: true,
  },
  {
    path: "/account",
    component: Account,
    children: [
      {
        path: "profile",
        component: AccountProfile,
      },
      {
        path: "/account/billing",
        component: AccountBilling,
      },
      {
        path: "delete",
        component: AccountDelete,
      },
    ],
  },
  {
    path: "/plugs",
    component: Plugs,
    exact: true,
  },
  {
    path: "/plugs/new",
    component: PlugsNew,
    exact: true,
  },
  {
    path: "/plug/:id",
    component: PlugDetail,
    children: [
      {
        path: "status",
        component: PlugStatus,
      },
      {
        path: "settings",
        component: PlugSettings,
      },
    ],
  },
]

export default routes