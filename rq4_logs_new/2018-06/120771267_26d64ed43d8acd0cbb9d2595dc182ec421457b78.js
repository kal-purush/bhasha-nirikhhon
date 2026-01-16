Vue.component('message', {
  template: '<input v-model = "message" @keyup.enter="saveMessage">',
  data: function() {
    return{
    message: ''
  }
},
methods: {
  saveMessage: function() {
    this.$emit('message-saved', this.message)

    this.message = ''
  }
}
})

new Vue({
    el: '#app',
    data: {
      messages: []
    },
    methods: {
      handleMessage: function(message) {
      this.messages.push(message)
      }
    }

})