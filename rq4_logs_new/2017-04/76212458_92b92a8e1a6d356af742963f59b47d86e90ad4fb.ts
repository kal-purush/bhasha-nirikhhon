import Vue from 'vue'
import Component from 'vue-class-component'

@Component({
  props: {
    orders: Array
  }
})
export default class Orders extends Vue {
}