/////////////////////////////////
////MODELS
/////////////////////////////////

var Game = Backbone.Model.extend({});

/////////////////////////////////

var User = Backbone.Model.extend({
    url: function () {
        return '/user/'+this.id;
    }
});

/////////////////////////////////
////VIEWS
/////////////////////////////////

var TopbarView = Backbone.View.extend({
  el: '#topbar',
  topbarTpl: _.template( $('#topbar-template').html()),

  render: function() {
    this.$el.html( this.topbarTpl( this.model.attributes ) );
    this.$('#games-list').append( this.model.attributes.games);
    return this;
  }
});

/////////////////////////////////

var DashView = Backbone.View.extend({
  el: '#display',
  dashTpl: _.template( $('#dash-template').html()),

  render: function () {
    this.$el.html( this.dashTpl( this.model.attributes ) );
    for(i=0;i<this.model.attributes.games.length;i++){
      this.$('#game_'+this.model.attributes.games[i]).click(function(){
        $('#display').html('Your game sir');
        var games = new GameCollection(); 
        games.fetch({success:function(m,r,o){
           console.log(games.toJSON());
        }});
      });
    }
    return this;
  }
});

/////////////////////////////////

var GameView = Backbone.View.extend({
  el: '#display',
  handTpl: _.template( $('#hand-template').html()),
  sidebarTpl: _.template( $('#sidebar-template').html()),
  gameboardTpl: _.template( $('#gameboard-template').html()),

  render: function() {
    this.$el.html( this.handTpl( this.model.attributes ) );
    var cards = this.model.attributes.cards;
    for(var i = 0; i < 6; i++){
        var targetDiv = '#card'+i.toString();
        //$(targetDiv).load('/static/images/'+cards[i]+'.svg');
        $(targetDiv).append('<h2>'+cards[i]+'</h2>');
    }
    return this;
  }
});

/////////////////////////////////
////COLLECTIONS
/////////////////////////////////

var GameCollection = Backbone.Collection.extend({
  model: Game,
  url: '/game'
});
