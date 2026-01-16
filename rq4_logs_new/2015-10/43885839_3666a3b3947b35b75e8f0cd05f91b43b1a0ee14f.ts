"use strict"
import component = require('vue-class-component')
import { Pagination } from './pagination'

interface Params {
  year: number
}

@component
export class Albums {
  static template = require('./albums.html')
  static components = { Pagination }

  year: number = (new Date).getFullYear()

  $route: VueRouter.$route<Params, any>

  static route = {
    data: function(transition: VueRouter.Transition) {
      var route: Params = transition.to.params
      setTimeout(() => {
        transition.next({
          year: Number(route.year)
        })
      }, 0)
    }
  }
}