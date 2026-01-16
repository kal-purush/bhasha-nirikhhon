$( document ).ready(function() {
    $('.daily-blurb').hide()
    $('.back').hide()
    $('.navbar').hide()
    $('.moonphase').hide()
    $('.sunrise').hide()
    $('.sunset').hide()
    $('.love-icon').hide()
    $('.career-blurb').hide()
    $('.wellness-blurb').hide()
    phaseMoon()
    horoscope()
    infoBox()
});

// function apodImage() {
//     $.ajax({
//     url:'https://api.nasa.gov/planetary/apod?api_key=xuyKIKtLax8H6CW7EP8vIJeDEZbLqga6rLLBOVD2',
//     }).done(function(data) {
//     $('#image').html('<img src="' + data.url + '" id="image" />');
//     });
// }

function phaseMoon() {

  navigator.geolocation.getCurrentPosition(success);

  function success(position) {
    var latitude = position.coords.latitude;
    var longitude = position.coords.longitude;

    var today = new Date();
    var dd = today.getDate();
    var mm = today.getMonth()+1;

    var yy = today.getFullYear();
      if(dd<10){
          dd='0'+dd
      }
      if(mm<10){
          mm='0'+mm
      }
    var today = dd+'/'+mm+'/'+yy;
    var date = $('#date').html(today);

    $.ajax({
    url:'https://api.aerisapi.com/sunmoon/?p=' + latitude + ',' + longitude + '&to=:' + today + '&client_id=id5zV9deaqOeGuK3mYadq&client_secret=Yfvc6cceJpNkt5aqmJB5i30VV1WfmS6z75d3hWRy',
    today: today,
    latitude: latitude,
    longitude: longitude
    }).done(function(data) {
    var moon = $('.moonphase').text(data.response['0'].moon.phase.name);
    var sunrise = ($('.sunrise').text(data.response['0'].sun.riseISO)).text();
    var sunset = ($('.sunset').text(data.response['0'].sun.setISO)).text();
    var rise = $('.sunrise').html('<p>rise ' + sunrise.substr(11, 8));
    var set = $('.sunset').html('<p>set ' + sunset.substr(11, 8));

      switch(moon.html()) {
        case 'waxing gibbous':
            $('.moon-icon').html('<img src="./images/waxing-gibbous.png" class="moon-icon" />');
            break;
        case 'waning gibbous':
            $('.moon-icon').html('<img src="./images/waning-gibbous.png" class="moon-icon" />')
            break;
        case 'waxing crescent':
            $('.moon-icon').html('<img src="./images/waxing-crescent.png" class="moon-icon" />')
            break;
        case 'waning crescent':
            $('.moon-icon').html('<img src="./images/waning-crescent.png" class="moon-icon" />')
            break;
        case 'full moon':
            $('.moon-icon').html('<img src="./images/full.png" class="moon-icon" />')
            break;
        case 'new moon':
            $('.moon-icon').html('<img src="./images/new.png" class="moon-icon" />')
            break;
        case 'first moon':
            $('.moon-icon').html('<img src="./images/first.png" class="moon-icon" />')
            break;
        case 'last moon':
            $('.moon-icon').html('<img src="./images/last.png" class="moon-icon"/>')
            break;
        }
    });
  };
}

function horoscope() {
  $.ajax({
  url:'https://zodia.herokuapp.com/zodiac',
}).done(function(data) {
    console.log(data);

    var cancer = data[3];
    var leo = data[4];
    var virgo = data[5];
    var libra = data[6];
    var scorpio = data[7];
    var sagittarius = data[8];
    var capricorn = data[9];
    var aquarius = data[10];
    var pisces = data[11];
    var aries = data[0];
    var taurus = data[1];
    var gemini = data[2];


    $('.cancer').click(function(e) {
      e.preventDefault()
      $('.wellness-blurb').html('<p>' + cancer.Wellness + '</p>')
      $('.career-blurb').html('<p>' + cancer.Career + '</p>')
      $('.love-blurb').html('<p>' + cancer.Love + '</p>')
      $('.sun-sign').html('<p class="current">' + cancer.Sign + '<p/>')
      $('.daily-blurb').html('<p>' + cancer.Daily_Horoscope + '<p/>')
      $('.ring').hide();
      $('.site-name').hide()
      $('.love-icon').show()
      $('.love-blurb').hide()
      $('.navbar').show()
      $('.sun-sign').show()
      $('.daily-blurb').show()
    })

    $('.leo').click(function(e) {
      $('.wellness-blurb').html('<p>' + leo.Wellness + '</p>')
      $('.career-blurb').html('<p>' + leo.Career + '</p>')
      $('.love-blurb').html('<p>' + leo.Love + '</p>')
      $('.sun-sign').html('<p class="current">' + leo.Sign + '</p>')
      $('.daily-blurb').html('<p>' + leo.Daily_Horoscope + '<p/>')
      $('.ring').hide();
      $('.site-name').hide()
      $('.love-icon').show()
      $('.love-blurb').hide()
      $('.navbar').show()
      $('.sun-sign').show()
      $('.daily-blurb').show();
    })

    $('.virgo').click(function(e) {
      e.preventDefault()
      $('.wellness-blurb').html('<p>' + virgo.Wellness + '</p>')
      $('.career-blurb').html('<p>' + virgo.Career + '</p>')
      $('.love-blurb').html('<p>' + virgo.Love + '</p>')
      $('.daily-blurb').html('<p>' + virgo.Daily_Horoscope + '<p/>')
      $('.sun-sign').html('<p class="current">' + virgo.Sign + '<p/>')
      $('.ring').hide();
      $('.site-name').hide()
      $('.love-icon').show()
      $('.love-blurb').hide()
      $('.navbar').show()
      $('.sun-sign').show()
      $('.daily-blurb').show();
    })

    $('.libra').click(function(e) {
      e.preventDefault()
      $('.wellness-blurb').html('<p>' + libra.Wellness + '</p>')
      $('.career-blurb').html('<p>' + libra.Career + '</p>')
      $('.love-blurb').html('<p>' + libra.Love + '</p>')
      $('.daily-blurb').html('<p>' + libra.Daily_Horoscope + '<p/>')
      $('.sun-sign').html('<p class="current">' + libra.Sign + '<p/>')
      $('.ring').hide();
      $('.site-name').hide()
      $('.love-icon').show()
      $('.love-blurb').hide()
      $('.navbar').show()
      $('.sun-sign').show()
      $('.daily-blurb').show();
    })

    $('.scorpio').click(function(e) {
      e.preventDefault()
      $('.wellness-blurb').html('<p>' + scorpio.Wellness + '</p>')
      $('.career-blurb').html('<p>' + scorpio.Career + '</p>')
      $('.love-blurb').html('<p>' + scorpio.Love + '</p>')
      $('.daily-blurb').html('<p>' + scorpio.Daily_Horoscope + '<p/>')
      $('.sun-sign').html('<p class="current">' + scorpio.Sign + '<p/>')
      $('.ring').hide();
      $('.site-name').hide()
      $('.love-icon').show()
      $('.love-blurb').hide()
      $('.navbar').show()
      $('.sun-sign').show()
      $('.daily-blurb').show();
    })

    $('.sagittarius').click(function(e) {
      e.preventDefault()
      $('.wellness-blurb').html('<p>' + sagittarius.Wellness + '</p>')
      $('.career-blurb').html('<p>' + sagittarius.Career + '</p>')
      $('.love-blurb').html('<p>' + sagittarius.Love + '</p>')
      $('.daily-blurb').html('<p>' + sagittarius.Daily_Horoscope + '<p/>')
      $('.sun-sign').html('<p class="current">' + sagittarius.Sign + '<p/>')
      $('.ring').hide();
      $('.site-name').hide()
      $('.love-icon').show()
      $('.love-blurb').hide()
      $('.navbar').show()
      $('.sun-sign').show()
      $('.daily-blurb').show();
    })

    $('.capricorn').click(function(e) {
      e.preventDefault()
      $('.wellness-blurb').html('<p>' + capricorn.Wellness + '</p>')
      $('.career-blurb').html('<p>' + capricorn.Career + '</p>')
      $('.love-blurb').html('<p>' + capricorn.Love + '</p>')
      $('.daily-blurb').html('<p>' + capricorn.Daily_Horoscope + '<p/>')
      $('.sun-sign').html('<p class="current">' + capricorn.Sign + '<p/>')
      $('.ring').hide();
      $('.site-name').hide()
      $('.love-icon').show()
      $('.love-blurb').hide()
      $('.navbar').show()
      $('.sun-sign').show()
      $('.daily-blurb').show();
    })

    $('.aquarius').click(function(e) {
      e.preventDefault()
      $('.wellness-blurb').html('<p>' + aquarius.Wellness + '</p>')
      $('.career-blurb').html('<p>' + aquarius.Career + '</p>')
      $('.love-blurb').html('<p>' + aquarius.Love + '</p>')
      $('.daily-blurb').html('<p>' + aquarius.Daily_Horoscope + '<p/>')
      $('.sun-sign').html('<p class="current">' + aquarius.Sign + '<p/>')
      $('.ring').hide();
      $('.site-name').hide()
      $('.love-icon').show()
      $('.love-blurb').hide()
      $('.navbar').show()
      $('.sun-sign').show()
      $('.daily-blurb').show();
    })

    $('.pisces').click(function(e) {
      e.preventDefault()
      $('.wellness-blurb').html('<p>' + pisces.Wellness + '</p>')
      $('.career-blurb').html('<p>' + pisces.Career + '</p>')
      $('.love-blurb').html('<p>' + pisces.Love + '</p>')
      $('.daily-blurb').html('<p>' + pisces.Daily_Horoscope + '<p/>')
      $('.sun-sign').html('<p class="current">' + pisces.Sign + '<p/>')
      $('.ring').hide();
      $('.site-name').hide()
      $('.love-icon').show()
      $('.love-blurb').hide()
      $('.navbar').show()
      $('.sun-sign').show()
      $('.daily-blurb').show();
    })

    $('.aries').click(function(e) {
      e.preventDefault()
      $('.wellness-blurb').html('<p>' + aries.Wellness + '</p>')
      $('.career-blurb').html('<p>' + aries.Career + '</p>')
      $('.love-blurb').html('<p>' + aries.Love + '</p>')
      $('.daily-blurb').html('<p>' + aries.Daily_Horoscope + '<p/>')
      $('.sun-sign').html('<p class="current">' + aries.Sign + '<p/>')
      $('.ring').hide();
      $('.site-name').hide()
      $('.love-blurb').hide()
      $('.navbar').show()
      $('.sun-sign').show()
      $('.daily-blurb').show();
    })

    $('.taurus').click(function(e) {
      e.preventDefault()
      $('.wellness-blurb').html('<p>' + taurus.Wellness + '</p>')
      $('.career-blurb').html('<p>' + taurus.Career + '</p>')
      $('.love-blurb').html('<p>' + taurus.Love + '</p>')
      $('.daily-blurb').html('<p>' + taurus.Daily_Horoscope + '<p/>')
      $('.sun-sign').html('<p class="current">' + taurus.Sign + '<p/>')
      $('.ring').hide();
      $('.site-name').hide()
      $('.love-icon').show()
      $('.love-blurb').hide()
      $('.navbar').show()
      $('.sun-sign').show()
      $('.daily-blurb').show();
    })

    $('.gemini').click(function(e) {
      e.preventDefault()
      $('.wellness-blurb').html('<p>' + gemini.Wellness + '</p>')
      $('.career-blurb').html('<p>' + gemini.Career + '</p>')
      $('.love-blurb').html('<p>' + gemini.Love + '</p>')
      $('.daily-blurb').html('<p>' + gemini.Daily_Horoscope + '<p/>')
      $('.sun-sign').html('<p class="current">' + gemini.Sign + '<p/>')
      $('.ring').hide();
      $('.site-name').hide()
      $('.love-icon').show()
      $('.love-blurb').hide()
      $('.navbar').show()
      $('.sun-sign').show()
      $('.daily-blurb').show();
    })

    $('.love-icon').click(function(e) {
      e.preventDefault()
      $('.daily-blurb').hide()
      $('.wellness-blurb').hide()
      $('.career-blurb').hide()
      $('.sunrise').hide()
      $('.sunset').hide()
      $('.moonphase').hide()
      $('.love-blurb').show()
      $('.love-blurb').html($('.current').Love)
    })

    $('.daily-icon').click(function(e) {
      e.preventDefault()
      $('.love-blurb').hide()
      $('.wellness-blurb').hide()
      $('.career-blurb').hide()
      $('.sunrise').hide()
      $('.sunset').hide()
      $('.moonphase').hide()
      $('.daily-blurb').show()
      $('.daily-blurb').html($('.current').Daily_Horoscope)
    })

    $('.wellness-icon').click(function(e) {
      e.preventDefault()
      $('.love-blurb').hide()
      $('.daily-blurb').hide()
      $('.career-blurb').hide()
      $('.sunrise').hide()
      $('.sunset').hide()
      $('.moonphase').hide()
      $('.wellness-blurb').show()
      $('.wellness-blurb').html($('.current').Wellness)
    })

    $('.career-icon').click(function(e) {
      e.preventDefault()
      $('.love-blurb').hide()
      $('.daily-blurb').hide()
      $('.wellness-blurb').hide()
      $('.sunrise').hide()
      $('.sunset').hide()
      $('.moonphase').hide()
      $('.career-blurb').show()
      $('.career-blurb').html($('.current').Career)
    })

    $('.circle-icon').click(function(e) {
      e.preventDefault()
      $('.navbar').hide()
      $('.ring').show()
      $('.site-name').show()
      $('.daily').hide()
      $('.love-blurb').hide()
      $('.daily-blurb').hide()
      $('.wellness-blurb').hide()
      $('.sunrise').hide()
      $('.sunset').hide()
      $('.moonphase').hide()
      $('.career-blurb').hide()
      $('.current').hide()
    })

  })
};

function infoBox() {
  $('.moon-icon').click(function() {
        $('.moonphase').show()
        $('.sunrise').hide()
        $('.sunset').hide()
    })
  $('.sun-icon').click(function() {
    $('.sunrise').show()
    $('.sunset').show()
    $('.moonphase').hide()
  })
};