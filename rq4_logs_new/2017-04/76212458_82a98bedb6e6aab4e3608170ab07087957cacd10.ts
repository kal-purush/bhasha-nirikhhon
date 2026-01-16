import Vue from 'vue'
import Component from 'vue-class-component'

import { GET_CART, REMOVE_LINE_ITEM, SET_LINE_ITEM_QUANTITY }
  from 'src/infrastructure/store/carts_types'

import LineItem from '../LineItem'

@Component({
  components: {
    LineItem
  }
})
export default class CardDetails extends Vue {
  get isEmpty() {
    const lineItems = this.cart.lineItems
    return (!lineItems) || (lineItems.length === 0)
  }

  // sotre operations
  get cart() {
    return this.$store.getters[GET_CART]
  }

  changeQuantity(data) {
    this.$store.dispatch(SET_LINE_ITEM_QUANTITY, data)
  }

  removeLineItem(data) {
    this.$store.dispatch(REMOVE_LINE_ITEM, data)
  }
}
