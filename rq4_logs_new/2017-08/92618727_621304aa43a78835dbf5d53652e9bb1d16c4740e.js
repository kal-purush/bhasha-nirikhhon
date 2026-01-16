document.addEventListener("DOMContentLoaded", function(event) { 
  var app = new Vue({
    el: '#app',
    data: {
      specials: [],
      selectedSpecials: []

    },
    mounted: function() {
    $.get('/api/v1/specials', function(response) {
        this.specials = response;
      }.bind(this));


    },
    methods: {
      selectSpecial: function(special) {
        console.log(special);
        this.selectSpecial.push(special);
      }

    }

  });
});