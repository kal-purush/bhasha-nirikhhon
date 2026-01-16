
vjs.TitleScreen = vjs.Component.extend({
    init: function (player, options) {
        vjs.Component.call(this, player, options);
        this.on(this.player_, 'firstplay', this.hide);
        this.on(this.player_, 'ended', this.showNext);
    }
});
vjs.TitleScreen.prototype.createEl = function() {
    var template = '<div class="container-fluid">'
                    +'<div class="row">'
                        +'<div class="col l7 m7 s12 ">'
                        +'<a class="card-panel stats-card brown brown-text text-lighten-5" href="#" ng-click="player.play()">'
                            +'<span class="title">{{currentVideo.title}}</span>'
                        +'</a>'
                        +'</div>'
                    +'</div>'
                    +'<div class="row">'
                        +'<div class="col l9 m9 s10">'
                        +'<a class="card-panel stats-card green lighten-2 green-text text-lighten-5" href="#" ng-click="player.play()">'
                            + '<p>{{currentVideo.description}}</p>'
                        +'</a>'
                    +'</div>'
                    +'<div class="col l3 m3 s2 ">'
                        +'<a class="card-panel play stats-card green darken-1 green-text text-lighten-5" href="#" ng-click="player.play()">'
                            +'<p><i class="fa fa-play fa-4x"></i></p>'
                        +'</a>'
                    +'</div>'
                    +'</div>'

                    +'<div class="row">'
                        +'<div class="col l9 m9 s10">'
                            +'<div class="card blue lighten-4 blue-text text-darken-2">'
                                +'<div class="title blue lighten-3 blue-text text-darken-3">'
                                + '<h5>Previous Video</h5>'
                                +'</div>'
                                    +'<div class="content">'
                                        +'<div class="col">'
                                        +'<a ui-sref="videojsview({index:itemIndex})">{{prevVideo.title}}</a>'

                                        +'</div>'
                                    +'</div>'
                            +'</div>'
                        +'</div>' //col
                     +'</div>' //row
        +'<div class="row">'
            +'<div class="col l9 m9 s10">'
                +'<div class="card blue lighten-4 blue-text text-darken-2">'
                    +'<div class="title blue lighten-3 blue-text text-darken-3">'
                        + '<h5>Next Video</h5>'
                    +'</div>'
                +'<div class="content">'
                    +'<div class="col">'
                        +'<a ui-sref="videojsview({index:itemIndex})">{{nextVideo.title}}</a>'
                    +'</div>'
                +'</div>'
            +'</div>' //col
        +'</div>'; //row


    var uiNode = jQuery('<div/>');
    uiNode.addClass('demo-vjs-titlescreen card-panel');
    uiNode.html(template);

    return uiNode[0];
};
vjs.TitleScreen.prototype.hide = function() {
    jQuery('.demo-vjs-titlescreen').fadeOut();
};
vjs.TitleScreen.prototype.showNext = function() {
    jQuery('.demo-vjs-titlescreen').fadeIn();
};