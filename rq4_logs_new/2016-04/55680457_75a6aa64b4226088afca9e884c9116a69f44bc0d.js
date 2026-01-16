
Router.configure({
    layoutTemplate: 'Layout'
});

Router.route('/',function(){
  this.render('playground',{
    to:'main'
  });
});

Template.playground.helpers({

  "allSounds":function(){

    return soundfiles;
  },

  "sound": function (soundId) {
     var soundIsOn = Session.get('sound'+soundId);
     var masterspeed = Session.get('masterspeed');
     var mastervolume = Session.get('mastervolume');
     var soundvolume = Session.get('vol'+soundId);
    var soundSpeed = (Session.get('speed'+soundId)/100)*(Session.get('masterspeed')/100);

      if (soundIsOn == 1 ) {
          playSound(soundId, (soundvolume/100) * (mastervolume/100),soundSpeed);

          $("#"+soundId).removeClass('btn-default').addClass('btn-success');
          $("#sb"+soundId).addClass('sbon');
      }
      else{
          playSound(soundId, 0,soundSpeed);
          $("#"+soundId).removeClass('btn-success').addClass('btn-default');
          $("#sb"+soundId).removeClass('sbon');
      }


  },
  "sliderSpeedVal": function() {
      var tmpl = Template.instance();
      var masterSlide = Session.get('masterspeed');
      if(tmpl.view.isRendered){
        tmpl.$('#sliderSpeed').data('uiSlider').value(masterSlide);
      }
      return (masterSlide/100).toFixed(2);
  },

  "sliderVolumeVal":  function() {

      var tmpl = Template.instance();
      var masterVolume = Session.get('mastervolume');
        if(tmpl.view.isRendered){
          tmpl.$('#sliderVolume').data('uiSlider').value(masterVolume);
        }
        return masterVolume;
  },
  "sliderVal":  function(soundId,type) { //type=volume or speed
    var s = Session.get('speed'+soundId);
    var v = Session.get('vol'+soundId);

      var tmpl = Template.instance();

          if(type=="volume"){

            if(tmpl.view.isRendered){  $('#'+soundId+'.sliderV').data('uiSlider').value(v);}


            return v;
          }
          else{

            if(tmpl.view.isRendered){  $('#'+soundId+'.sliderS').data('uiSlider').value(s);}
            return (s/100).toFixed(2);
          }

  }
});


Template.playground.events({

  "click .js-start-dac": function () {
    Session.set('startdac', 1);

    playAll();
    $('.js-start-dac').prop('disabled', true);
    $('.js-stop-dac').prop('disabled', false);
  },
  "click .js-stop-dac":function(){
    Session.set('startdac', 0);

    stopAll(0);
    $('.js-start-dac').prop('disabled', false);
    $('.js-stop-dac').prop('disabled', true);
  },
  "click .js-sound-onoff": function (e) {

    if(Session.get('startdac') == 1){
      var soundId = e.target.id;

      if(!Session.get('sound'+soundId)){
        Session.set('sound'+soundId, 1);
        if(Session.get('vol'+soundId) == undefined){
            Session.set('vol'+soundId, 50);
        }
        if(Session.get('speed'+soundId) == undefined){
            Session.set('speed'+soundId, 100);
        }

      }
      else{
        Session.set('sound'+soundId, 0);

      }

    }


  },
  "click .js-resync":function(e){

    if(Session.get('startdac')){
    //  Session.set('startdac',2);
      stopAll(1);
      playAll();
    }
  },

});

Template.playground.onRendered(function() {
  $('.js-stop-dac').prop('disabled', true);

  if(!this.$('.slider').data('uiSlider')){
    $(".slider").slider({
        min: 0,
    });
  }

  var handlerspeed = _.throttle(function(event, ui) {
    Session.set('masterspeed', ui.value);


  }, 50, { leading: false });

  var handlervolume = _.throttle(function(event, ui) {

    Session.set('mastervolume', ui.value);
  }, 50, { leading: false });


  var handlerV = _.throttle(function(event, ui) {

      var soundId = event.target.id;

      Session.set('vol'+soundId, ui.value);

  }, 50, { leading: false });

  var handlerS = _.throttle(function(event, ui) {

    var soundId = event.target.id;
    Session.set('speed'+soundId, ui.value);

  }, 50, { leading: false });


  $("#sliderSpeed").slider({
     slide: handlerspeed,
      max: 200

  });

  $("#sliderVolume").slider({
      slide: handlervolume,
      max: 100
  });

  $(".sliderV").slider({

    slide:handlerV,
    max: 100
  });



  $(".sliderS").slider({
    slide:handlerS,
    max: 200
  });

  Session.set('mastervolume', 50);
  Session.set('masterspeed', 100);
  for(var i = 0 ; i < soundfiles.length; i++){
  	Session.set('vol'+i, 50);
  	Session.set('speed'+i, 100);
  }
});