// PLAYER DETAILS (global registration)
Vue.component('player-details', {
  data: function () {
    return {
      playerList: [
        {
          name: 'Alessandra', 
          title: 'The Champion', 
          age: 19,
        },
        {
          name: 'Dani', 
          title: 'Second Place', 
          age: 15,
        },
                                    
      ]
    };
  },
    props: ['title'],
    template: 
      `<div><div class="player" v-for="player in playerList"> 
      <span>Name: {{player.name}}</span> <br>
      <span>Title: {{player.title}}</span> <br>
      <span>Age: {{player.age}}</span> </div></div>`,
    template: '<h1> {{title}} </h1>'
});

// PLAYER DETAILS (local registration)                         
const PlayerDet = {
  data: function () {
    return {
      playerList: [
        {
          name: 'Domi', 
          title: 'The Survivor', 
          age: 19,
        },
        {
          name: 'Desi', 
          title: 'Moral Support', 
          age: 4,
        },
                                   
      ]
    };
  },
  template: 
    `<div><div class="player" v-for="player in playerList"> 
    <span>Name: {{player.name}}</span> <br>
    <span>Title: {{player.title}}</span> <br>
    <span>Age: {{player.age}}</span> </div></div>`
};
                            
var app = new Vue(
  {
    el: '#details',
    data: {
      heading: "Player details",
    },
    components: {
      'play-det': PlayerDet
    },
});
                            
// PLAYER ID (custom events)
Vue.component('player-id',
  {
    template: 
      `<div> <input placeholder="Enter player ID"> 
      <button @click="onidEntered">Confirm</button> <div>`,
    methods:
    {
      onidEntered() {
        this.$emit('applied');
      }
    }
  });

var playerID = new Vue(
  {
    el: '#idnumber',
    data: {
      heading: 'Enter player ID',
      idEntered: false
    },
    methods:
    {
      onidEntered() {
        this.idEntered = true;
      }
    }
  }
);

// PLAYER WEAPONS (slots)
Vue.component("weapon-det", 
  {
    template: 
    `<div class="weapContainer">

        <div id="weapon-name">
          <slot name="name"></slot>
        </div>

        <div id="weapon-photo">
          <slot name="photo"></slot>
        </div>

        <div id="weapon-desc">
          <slot name="desc"></slot>
        </div>

    </div>`
  });

var weaponDet = new Vue(
  {
    el: 'weapon',
  });
  
                        
                            