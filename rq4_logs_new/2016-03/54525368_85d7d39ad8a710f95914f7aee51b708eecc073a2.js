var SunCalc = {};


var WEATHER_THUNDER  = 200;
var WEATHER_DRIZZLE  = 300;
var WEATHER_RAIN     = 500;
var WEATHER_SNOW     = 600;
var WEATHER_ATMOS    = 700;
var WEATHER_CLEAR    = 800;
var WEATHER_CLOUDS   = 802;
var WEATHER_WIND     = 950;
var WEATHER_EXTREME  = 900;

var _use_cust_latlon = localStorage.getItem('use_cust_latlon')==='T' ? true : false;
var _use_cust_lat = localStorage.getItem('cust_lat') ? localStorage.getItem('cust_lat') : '';
var _use_cust_lon = localStorage.getItem('cust_lon') ? localStorage.getItem('cust_lon') : '';

var spool_dict = {};
var spool_delay = null;

function push_message (title, dict) {
  // spool delay
  var delay = 1000;
  console.log('title', JSON.stringify(dict));
  
  // push data to list
  for (var key in dict) {
     if (dict.hasOwnProperty(key)) {
       spool_dict[key] = dict[key];
     }
  }

  // Cancel timeout, if there is one
  if (spool_delay) clearTimeout(spool_delay);
  // Start timeout delay
  spool_delay = setTimeout(function () {
    _send_messages();
  }, delay);

}
function _send_messages () {
//   console.log('sending messages', JSON.stringify(spool_dict));
  // Send to Pebble
  Pebble.sendAppMessage(spool_dict,
    function(e) {
      console.log("Info sent to pebble!");
    },
    function(e) {
      console.log("Error sending info to Pebble!");
    }
  );
  spool_dict = {};
}


Pebble.addEventListener('showConfiguration', function(e) {
  var color_background = localStorage.getItem('color_background') ? localStorage.getItem('color_background') : '0x000000';
  var color_face = localStorage.getItem('color_face') ? localStorage.getItem('color_face') : '0x000000';
  var color_sweep = localStorage.getItem('color_sweep') ? localStorage.getItem('color_sweep') : '0x0055FF';
  var color_outline = localStorage.getItem('color_outline') ? localStorage.getItem('color_outline') : '0x0055FF';
  var color_riseset = localStorage.getItem('color_riseset') ? localStorage.getItem('color_riseset') : '0x0055FF';
  var color_ticks = localStorage.getItem('color_ticks') ? localStorage.getItem('color_ticks') : '0xFFFFFF';
  var color_apex = localStorage.getItem('color_apex') ? localStorage.getItem('color_apex') : '0xFFFFFF';
  var color_text = localStorage.getItem('color_text') ? localStorage.getItem('color_text') : '0xFFFFFF';
  var color_text_alt = localStorage.getItem('color_text_alt') ? localStorage.getItem('color_text_alt') : '0xFFFFFF';
  var temp_format = localStorage.getItem('temp_format') ? localStorage.getItem('temp_format') : 'F';
  var show_weather = localStorage.getItem('show_weather') ? localStorage.getItem('show_weather') : 'T';
  var show_hour_arrow = localStorage.getItem('show_hour_arrow') ? localStorage.getItem('show_hour_arrow') : 'T';
  var show_battery = localStorage.getItem('show_battery') ? localStorage.getItem('show_battery') : 'F';
  var noon_low = localStorage.getItem('noon_low') ? localStorage.getItem('noon_low') : 'T';
  var use_cust_latlon = localStorage.getItem('use_cust_latlon') ? localStorage.getItem('use_cust_latlon') : 'F';
  var cust_lat = localStorage.getItem('cust_lat') ? localStorage.getItem('cust_lat') : '';
  var cust_lon = localStorage.getItem('cust_lon') ? localStorage.getItem('cust_lon') : '';

  var url = 'http://2460.kissr.com/' + 
     '?color_background=' + color_background +
     '&color_face=' + color_face +
     '&color_sweep=' + color_sweep +
     '&color_outline=' + color_outline +
     '&color_riseset=' + color_riseset +
     '&color_ticks=' + color_ticks +
     '&color_apex=' + color_apex +
     '&color_text=' + color_text +
     '&color_text_alt=' + color_text_alt +
     '&temp_format=' + temp_format +
     '&show_weather=' + show_weather +
     '&show_hour_arrow=' + show_hour_arrow +
     '&show_battery=' + show_battery +
     '&noon_low=' + noon_low +
     '&use_cust_latlon=' + use_cust_latlon +
     '&cust_lat=' + cust_lat +
     '&cust_lon=' + cust_lon;
                
  // Show config page
  Pebble.openURL(url);
});

Pebble.addEventListener('webviewclosed', function(e) {
  if (!e || !e.response) return false;
  // Decode and parse config data as JSON
  var config_data = JSON.parse(decodeURIComponent(e.response));
//   console.log('Config window returned: ', JSON.stringify(config_data));



  
  localStorage.setItem('color_background', config_data["color_background"]);
  localStorage.setItem('color_face', config_data["color_face"]);
  localStorage.setItem('color_text', config_data["color_text"]);
  localStorage.setItem('color_text_alt', config_data["color_text_alt"]);
  localStorage.setItem('color_sweep', config_data["color_sweep"]);
  localStorage.setItem('color_outline', config_data["color_outline"]);
  localStorage.setItem('color_ticks', config_data["color_ticks"]);
  localStorage.setItem('color_apex', config_data["color_apex"]);
  localStorage.setItem('color_riseset', config_data["color_riseset"]);
  localStorage.setItem('temp_format', config_data["temp_format"]);
  localStorage.setItem('show_weather', config_data["show_weather"]);  
  localStorage.setItem('show_hour_arrow', config_data["show_hour_arrow"]);  
  localStorage.setItem('show_battery', config_data["show_battery"]);
  localStorage.setItem('noon_low', config_data["noon_low"]);
  localStorage.setItem('use_cust_latlon', config_data["use_cust_latlon"]);
  localStorage.setItem('cust_lat', config_data["cust_lat"]);
  localStorage.setItem('cust_lon', config_data["cust_lon"]);
  
  _use_cust_latlon = false;
  if (config_data["use_cust_latlon"] === "T") {
    _use_cust_latlon = true;
    _use_cust_lat = config_data["cust_lat"];
    _use_cust_lon = config_data["cust_lon"];
  }
  
  getLocation();
  
  push_message("config data", {
    'KEY_ANIMATIONS': config_data["animSetting"],
    'KEY_TICK': config_data["tickSetting"],
    'KEY_BACKGROUND_COLOR': config_data["bgColor"],
    'KEY_COLOR_BACKGROUND': config_data["color_background"],
    'KEY_COLOR_FACE': config_data["color_face"],
    'KEY_COLOR_TEXT': config_data["color_text"],
    'KEY_COLOR_TEXT_ALT': config_data["color_text_alt"],
    'KEY_COLOR_SWEEP': config_data["color_sweep"],
    'KEY_COLOR_OUTLINE': config_data["color_outline"],
    'KEY_COLOR_TICKS': config_data["color_ticks"],
    'KEY_COLOR_APEX': config_data["color_apex"],
    'KEY_COLOR_RISESET': config_data["color_riseset"],
    'KEY_TEMP_FORMAT': config_data["temp_format"],
    'KEY_SHOW_WEATHER': config_data["show_weather"],
    'KEY_SHOW_HOUR_ARROW': config_data["show_hour_arrow"], 
    'KEY_SHOW_BATTERY': config_data["show_battery"],
    'KEY_NOON_LOW': config_data["noon_low"] 
  });

});



var xhrRequest = function (url, type, callback) {
  var xhr = new XMLHttpRequest();
  xhr.onload = function () {
    callback(this.responseText);
  };
  xhr.open(type, url);
  xhr.send();
};




function getSunPos(pos) {

  var lat = _use_cust_latlon ? _use_cust_lat : pos.coords.latitude;
  var lon = _use_cust_latlon ? _use_cust_lon : pos.coords.longitude;

  
  console.log('use cust latlon', _use_cust_latlon, lat, lon);  
  
  
  var times = SunCalc.getTimes(new Date(), lat, lon);

  // format sunrise time from the Date object
  var minutes = times.sunrise.getMinutes();
  if (minutes < 10) minutes = "0" + minutes;
  var sunrise = parseInt(times.sunrise.getHours() + '' + minutes);

  minutes = times.sunset.getMinutes();
  if (minutes < 10) minutes = "0" + minutes;
  var sunset = parseInt(times.sunset.getHours() + '' + minutes);


  push_message("sun position", {
    "KEY_SUNRISE": sunrise,
    "KEY_SUNSET": sunset
  });
  
}


// TODO: Send app message, dictionary, all at once...
// Can I push dictionary items to a method that gets called at the end of all possible items?
// How do know if we're at the end of the list and can sendAppMessage?
// I know I do sunrise/set and weather on launch and on intervals
// Changing colors fires on closing setting window - and also calls weather and sunrise/set
// Perhaps the push method can have a "finished" flag?


function getWeather(pos) {
  
  var temp_unit = 'imperial';
  var temp_format = localStorage.getItem('temp_format') ? localStorage.getItem('temp_format') : 'F';
  if (temp_format === 'C') temp_unit = 'metric';
  
  var lat = _use_cust_latlon ? _use_cust_lat : pos.coords.latitude;
  var lon = _use_cust_latlon ? _use_cust_lon : pos.coords.longitude;
  
  //   console.log('coords', pos.coords.latitude, pos.coords.longitude);
  // Construct URL
  var url = 'http://api.openweathermap.org/data/2.5/weather' +
    '?units=' + temp_unit +
    '&lat=' + lat +
    '&lon=' + lon +
    '&APPID=47275c0cb681ef2394b5ad0bad0b9559';

//   console.log('url', url);
  
  // Send request to OpenWeatherMap
  xhrRequest(url, 'GET', 
    function(responseText) {
      // responseText contains a JSON object with weather info
      var json = JSON.parse(responseText);

      // Temperature
      var temperature = Math.round(json.main.temp);
//       console.log("Temperature is " + temperature);

      // Max temp
      var temp_max = Math.round(json.main.temp_max);
//       console.log("Max temp is " + temp_max);      
      
      // Conditions
//       var conditions = json.weather[0].main;
      var condId = parseInt(json.weather[0].id);
//       console.log("Conditions are " + conditions, condId);
          
      var condition_code = 0;      
      if (condId >= 200 && condId <= 299) condition_code = WEATHER_THUNDER;
      if (condId >= 300 && condId <= 399) condition_code = WEATHER_DRIZZLE;      
      if (condId >= 500 && condId <= 599) condition_code = WEATHER_RAIN;
      if (condId >= 600 && condId <= 699) condition_code = WEATHER_SNOW;
      if (condId >= 700 && condId <= 799) condition_code = WEATHER_ATMOS;
      if (condId >= 800 && condId <= 801) condition_code = WEATHER_CLEAR;
      if (condId >= 802 && condId <= 899) condition_code = WEATHER_CLOUDS;
      if (condId >= 900 && condId <= 999) condition_code = WEATHER_EXTREME;
      if (condId >= 950 && condId <= 956) condition_code = WEATHER_WIND; // Will trump extreme
      
      push_message("weather info", {
        "KEY_TEMPERATURE": temperature,
        //"KEY_CONDITIONS": conditions,
        "KEY_CONDITION_CODE": condition_code,
        "KEY_TEMP_MAX": temp_max
      });

    }      
  );
}


function locationSuccess(pos) {
  getSunPos(pos); 
  
  // Weather
  setTimeout(function () {
    getWeather(pos);
  }, 200);
}

function locationError(err) {
  console.log("Error requesting location!");
}

function getLocation() {
  navigator.geolocation.getCurrentPosition(
    locationSuccess,
    locationError,
    {timeout: 15000, maximumAge: 60000}
  );
}





// Listen for when the watchface is opened
Pebble.addEventListener('ready', 
  function(e) {
//     console.log("PebbleKit JS ready!");
    
    // Get weather and refresh on interval
    getLocation();
    // Get the initial weather
    setInterval( function () {
      getLocation();
    }, 3600000); // Refresh every 30 minutes
    
  }
);



// Listen for when an AppMessage is received
Pebble.addEventListener('appmessage',
  function(e) {
//     console.log("AppMessage received!");
    getLocation();
  }                     
);












/*
 (c) 2011-2015, Vladimir Agafonkin
 SunCalc is a JavaScript library for calculating sun/moon position and light phases.
 https://github.com/mourner/suncalc
*/

(function () { 'use strict';

// shortcuts for easier to read formulas

var PI   = Math.PI,
    sin  = Math.sin,
    cos  = Math.cos,
    tan  = Math.tan,
    asin = Math.asin,
    atan = Math.atan2,
    acos = Math.acos,
    rad  = PI / 180;

// sun calculations are based on http://aa.quae.nl/en/reken/zonpositie.html formulas


// date/time constants and conversions

var dayMs = 1000 * 60 * 60 * 24,
    J1970 = 2440588,
    J2000 = 2451545;

function toJulian(date) { return date.valueOf() / dayMs - 0.5 + J1970; }
function fromJulian(j)  { return new Date((j + 0.5 - J1970) * dayMs); }
function toDays(date)   { return toJulian(date) - J2000; }


// general calculations for position

var e = rad * 23.4397; // obliquity of the Earth

function rightAscension(l, b) { return atan(sin(l) * cos(e) - tan(b) * sin(e), cos(l)); }
function declination(l, b)    { return asin(sin(b) * cos(e) + cos(b) * sin(e) * sin(l)); }

function azimuth(H, phi, dec)  { return atan(sin(H), cos(H) * sin(phi) - tan(dec) * cos(phi)); }
function altitude(H, phi, dec) { return asin(sin(phi) * sin(dec) + cos(phi) * cos(dec) * cos(H)); }

function siderealTime(d, lw) { return rad * (280.16 + 360.9856235 * d) - lw; }


// general sun calculations

function solarMeanAnomaly(d) { return rad * (357.5291 + 0.98560028 * d); }

function eclipticLongitude(M) {

    var C = rad * (1.9148 * sin(M) + 0.02 * sin(2 * M) + 0.0003 * sin(3 * M)), // equation of center
        P = rad * 102.9372; // perihelion of the Earth

    return M + C + P + PI;
}

function sunCoords(d) {

    var M = solarMeanAnomaly(d),
        L = eclipticLongitude(M);

    return {
        dec: declination(L, 0),
        ra: rightAscension(L, 0)
    };
}


// var SunCalc = {};


// calculates sun position for a given date and latitude/longitude

SunCalc.getPosition = function (date, lat, lng) {

    var lw  = rad * -lng,
        phi = rad * lat,
        d   = toDays(date),

        c  = sunCoords(d),
        H  = siderealTime(d, lw) - c.ra;

    return {
        azimuth: azimuth(H, phi, c.dec),
        altitude: altitude(H, phi, c.dec)
    };
};


// sun times configuration (angle, morning name, evening name)

var times = SunCalc.times = [
    [-0.833, 'sunrise',       'sunset'      ],
    [  -0.3, 'sunriseEnd',    'sunsetStart' ],
    [    -6, 'dawn',          'dusk'        ],
    [   -12, 'nauticalDawn',  'nauticalDusk'],
    [   -18, 'nightEnd',      'night'       ],
    [     6, 'goldenHourEnd', 'goldenHour'  ]
];

// adds a custom time to the times config

SunCalc.addTime = function (angle, riseName, setName) {
    times.push([angle, riseName, setName]);
};


// calculations for sun times

var J0 = 0.0009;

function julianCycle(d, lw) { return Math.round(d - J0 - lw / (2 * PI)); }

function approxTransit(Ht, lw, n) { return J0 + (Ht + lw) / (2 * PI) + n; }
function solarTransitJ(ds, M, L)  { return J2000 + ds + 0.0053 * sin(M) - 0.0069 * sin(2 * L); }

function hourAngle(h, phi, d) { return acos((sin(h) - sin(phi) * sin(d)) / (cos(phi) * cos(d))); }

// returns set time for the given sun altitude
function getSetJ(h, lw, phi, dec, n, M, L) {

    var w = hourAngle(h, phi, dec),
        a = approxTransit(w, lw, n);
    return solarTransitJ(a, M, L);
}


// calculates sun times for a given date and latitude/longitude

SunCalc.getTimes = function (date, lat, lng) {

    var lw = rad * -lng,
        phi = rad * lat,

        d = toDays(date),
        n = julianCycle(d, lw),
        ds = approxTransit(0, lw, n),

        M = solarMeanAnomaly(ds),
        L = eclipticLongitude(M),
        dec = declination(L, 0),

        Jnoon = solarTransitJ(ds, M, L),

        i, len, time, Jset, Jrise;


    var result = {
        solarNoon: fromJulian(Jnoon),
        nadir: fromJulian(Jnoon - 0.5)
    };

    for (i = 0, len = times.length; i < len; i += 1) {
        time = times[i];

        Jset = getSetJ(time[0] * rad, lw, phi, dec, n, M, L);
        Jrise = Jnoon - (Jset - Jnoon);

        result[time[1]] = fromJulian(Jrise);
        result[time[2]] = fromJulian(Jset);
    }

    return result;
};


// moon calculations, based on http://aa.quae.nl/en/reken/hemelpositie.html formulas

function moonCoords(d) { // geocentric ecliptic coordinates of the moon

    var L = rad * (218.316 + 13.176396 * d), // ecliptic longitude
        M = rad * (134.963 + 13.064993 * d), // mean anomaly
        F = rad * (93.272 + 13.229350 * d),  // mean distance

        l  = L + rad * 6.289 * sin(M), // longitude
        b  = rad * 5.128 * sin(F),     // latitude
        dt = 385001 - 20905 * cos(M);  // distance to the moon in km

    return {
        ra: rightAscension(l, b),
        dec: declination(l, b),
        dist: dt
    };
}

SunCalc.getMoonPosition = function (date, lat, lng) {

    var lw  = rad * -lng,
        phi = rad * lat,
        d   = toDays(date),

        c = moonCoords(d),
        H = siderealTime(d, lw) - c.ra,
        h = altitude(H, phi, c.dec);

    // altitude correction for refraction
    h = h + rad * 0.017 / tan(h + rad * 10.26 / (h + rad * 5.10));

    return {
        azimuth: azimuth(H, phi, c.dec),
        altitude: h,
        distance: c.dist
    };
};


// calculations for illumination parameters of the moon,
// based on http://idlastro.gsfc.nasa.gov/ftp/pro/astro/mphase.pro formulas and
// Chapter 48 of "Astronomical Algorithms" 2nd edition by Jean Meeus (Willmann-Bell, Richmond) 1998.

SunCalc.getMoonIllumination = function (date) {

    var d = toDays(date),
        s = sunCoords(d),
        m = moonCoords(d),

        sdist = 149598000, // distance from Earth to Sun in km

        phi = acos(sin(s.dec) * sin(m.dec) + cos(s.dec) * cos(m.dec) * cos(s.ra - m.ra)),
        inc = atan(sdist * sin(phi), m.dist - sdist * cos(phi)),
        angle = atan(cos(s.dec) * sin(s.ra - m.ra), sin(s.dec) * cos(m.dec) -
                cos(s.dec) * sin(m.dec) * cos(s.ra - m.ra));

    return {
        fraction: (1 + cos(inc)) / 2,
        phase: 0.5 + 0.5 * inc * (angle < 0 ? -1 : 1) / Math.PI,
        angle: angle
    };
};


function hoursLater(date, h) {
    return new Date(date.valueOf() + h * dayMs / 24);
}

// calculations for moon rise/set times are based on http://www.stargazing.net/kepler/moonrise.html article

SunCalc.getMoonTimes = function (date, lat, lng) {
    var t = new Date(date);
    t.setHours(0);
    t.setMinutes(0);
    t.setSeconds(0);
    t.setMilliseconds(0);

    var hc = 0.133 * rad,
        h0 = SunCalc.getMoonPosition(t, lat, lng).altitude - hc,
        h1, h2, rise, set, a, b, xe, ye, d, roots, x1, x2, dx;

    // go in 2-hour chunks, each time seeing if a 3-point quadratic curve crosses zero (which means rise or set)
    for (var i = 1; i <= 24; i += 2) {
        h1 = SunCalc.getMoonPosition(hoursLater(t, i), lat, lng).altitude - hc;
        h2 = SunCalc.getMoonPosition(hoursLater(t, i + 1), lat, lng).altitude - hc;

        a = (h0 + h2) / 2 - h1;
        b = (h2 - h0) / 2;
        xe = -b / (2 * a);
        ye = (a * xe + b) * xe + h1;
        d = b * b - 4 * a * h1;
        roots = 0;

        if (d >= 0) {
            dx = Math.sqrt(d) / (Math.abs(a) * 2);
            x1 = xe - dx;
            x2 = xe + dx;
            if (Math.abs(x1) <= 1) roots++;
            if (Math.abs(x2) <= 1) roots++;
            if (x1 < -1) x1 = x2;
        }

        if (roots === 1) {
            if (h0 < 0) rise = i + x1;
            else set = i + x1;

        } else if (roots === 2) {
            rise = i + (ye < 0 ? x2 : x1);
            set = i + (ye < 0 ? x1 : x2);
        }

        if (rise && set) break;

        h0 = h2;
    }

    var result = {};

    if (rise) result.rise = hoursLater(t, rise);
    if (set) result.set = hoursLater(t, set);

    if (!rise && !set) result[ye > 0 ? 'alwaysUp' : 'alwaysDown'] = true;

    return result;
};

}());