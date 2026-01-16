import Vue, { Component } from 'vue'
import { mapActions, mapGetters } from 'vuex'
import { GET_CART, REMOVE_LINE_ITEM, SET_LINE_ITEM_QUANTITY } from 'src/infrastructure/store/carts_types'

import LineItem from 'src/components/cart/LineItem'
import OrderSummary from 'src/components/cart/OrderSummary'

export default {
  name: 'checkout',

  computed: {
    ...mapGetters( {
      cart: GET_CART
    } ),

    isEmpty ( this: Component ) {
      const lineItems = this['cart'].lineItems
      return ( !lineItems ) || ( lineItems.length === 0 )
    }
  },

  methods: {
    ...mapActions ( {
      changeQuantity: SET_LINE_ITEM_QUANTITY,
      removeLineItem: REMOVE_LINE_ITEM
    } )

  },

  components: {
    LineItem,
    OrderSummary
  }
} as Vue.ComponentOptions<Vue>