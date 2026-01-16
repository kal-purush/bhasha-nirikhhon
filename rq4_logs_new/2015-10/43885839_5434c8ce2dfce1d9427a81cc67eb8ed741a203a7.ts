export function configureRouter(router: VueRouter.Router) {
  router.map({
    "/": {
      component: { template: "<div>top page</div>" }
    },
    "/albums": {
      component: { template: "<div>albums</div>" }
    },
    "/albums/:id": {
      component: { template: "<div>an album</div>" }
    },
    "/songs": {
      component: { template: "<div>songs and search bar</div>" }
    },
    "/songs/:id": {
      component: { template: "<div>a song</div>" }
    },
    "/settings": {
      component: { template: "<div>settings</div>" }
    }
  })
}