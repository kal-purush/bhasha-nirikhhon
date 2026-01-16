app.component('filter-form', {
    template: 
    /*html*/
    `<div class="filter-menu">
        <form class="filter-form" @submit.prevent="onSubmit">
            <h3>Rechercher un pokémon</h3>
            <input id="name" v-model="name" placeholder="Nom ou id ...">
            <input class="button" type="submit" value="Recherche">
        </form>
    </div>`,
    data: function() {
      return {
          name: ''
      }
    },
    methods: {
        onSubmit() {
            this.$emit('research-pokemon', this.name)
        }
    }
  })