var rivals = rivals || {};
rivals.utils = rivals.utils || {};
rivals.jsonTimes = [
	{ Name: '~ca~[R]~is~', Timezone: -11 },
	{ Name: '[R]JAXX', Timezone: -10 },
	{ Name: '[R]emnant', Timezone: -6 },
	{ Name: '[R] BasedGod', Timezone: -5 },
	{ Name: '[R]Camila', Timezone: -5 },
	{ Name: 'Nor[R]th', Timezone: -4 },
	{ Name: '[R] Rayren21', Timezone: -4 },
	{ Name: '[R] Zylo', Timezone: -4 },
	{ Name: '[R] Syl', Timezone: -2 },
	{ Name: '[R]~*CRA*~[R]', Timezone: 0 },
	{ Name: '[R]johns', Timezone: 1 },
	{ Name: '[R] Shanks', Timezone: 1 },
	{ Name: '[R]Arryn', Timezone: 2 },
	{ Name: '[R] Avelane', Timezone: 2 },
	{ Name: '[R]Bernado', Timezone: 2 },
	{ Name: '[R]Deadline', Timezone: 2 },
	{ Name: '[R] El Greco', Timezone: 2 },
	{ Name: '[R]Gerrard', Timezone: 2 },
	{ Name: '[R]Hollie', Timezone: 2 },
	{ Name: '[R]Space', Timezone: 2 },
	{ Name: '[R]Cool', Timezone: 2 },
	{ Name: '[R]Ace♠', Timezone: 3 },
	{ Name: '[R] AⓇmin', Timezone: 3 },
	{ Name: '[R]Almog', Timezone: 3 },
	{ Name: 'FisHunte[R]Ⓡ', Timezone: 3 },
	{ Name: '[R] †G†', Timezone: 3 },
	{ Name: '[R]lpeksi', Timezone: 3 },
	{ Name: '[R]NastyCloud', Timezone: 3 },
	{ Name: '[R]NetanelG', Timezone: 3 },
	{ Name: '[R] Rumborak', Timezone: 3 },
	{ Name: '[R] Holy', Timezone: 4 },
	{ Name: '[R]Prophesy', Timezone: 4 },
	{ Name: '[R] whatever', Timezone: 4 },
	{ Name: '[R]EasY', Timezone: 5 },
	{ Name: '[R]Nightmare', Timezone: 5 },
	{ Name: '[R] WoOopS', Timezone: 5.5 },
	{ Name: '(R) Toriki', Timezone: 6 },
	{ Name: '[R]Đôtế', Timezone: 7 },
	{ Name: '[R] Loop', Timezone: 7 },
	{ Name: '[R]YuYman', Timezone: 7 },
	{ Name: '[R]aven', Timezone: 7 },
	{ Name: '[R] Andy', Timezone: 8 },
	{ Name: '[R]ivals', Timezone: 8 },
	{ Name: '[R] u there', Timezone: 9 },
	{ Name: '[R]Zarellis', Timezone: 9.5 },
	{ Name: '[R[Vitold', Timezone: 10 },
	{ Name: '[R]Chip', Timezone: 12 },
	{ Name: '[R]anos', Timezone: null },
	{ Name: '[R]aven', Timezone: null },
	{ Name: '[R]GR!NGO', Timezone: null },
	{ Name: '[R]Xarantha', Timezone: null }
];

// Basic timezones
rivals.timeZones = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, -11, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1];
rivals.halfTimeZones = [5.5, 9.5];
rivals.rewardTime =  {hour: 21, minute: 5};
rivals.systemPassword = "ec4dcd22d050931ab78b43ae3dbef490a0ad614a";
rivals.debug = false;

$(function() {
  rivals.displayTimeline();
  
  $('#toggleView').on('click', function() {
    rivals.displayTimeZones();
  });
});

rivals.setRewards = function ($div, lowerOffset, upperOffset) {
  var heroes = [];
  
  var singleLog = true;     
  $(rivals.jsonTimes).each(function(i, v) {
    if(typeof v.Timezone !== 'undefined' && v.Timezone !== null) {
      var currentLocalTime = moment.utc().add({hours: v.Timezone});
      
      var r = $.extend({}, rivals.rewardTime, {day: currentLocalTime.date()});
      var targetUpper = moment.utc(r).add(upperOffset);
      var targetLower = moment.utc(r).add(lowerOffset);
                      
      var log = '';
      log += '[' + v.Timezone + '] ' + currentLocalTime.format('MM-DD H:mm-Z'); 
      log += ' => (' + targetLower.format('MM-DD H:mm-Z') + ') & (' + targetUpper.format('MM-DD H:mm-Z') + ') [' + currentLocalTime.isBetween(targetLower, targetUpper) + ']';  
      rivals.log(log);
        
      if(currentLocalTime.isBetween(targetLower, targetUpper)) {
        heroes.push(v);
      }
    }    
  });
  
  var $p = $div.find('p');   
	$p.html('');
	
	rivals.loadHeroes($p, heroes);
  	
  
  if ($p.html() == '') {
		$p.html('No one has a reward at this time');
	}
	else {
		var $span = $p.find('span:last'), text = $span.html();
		$span.html(text.substring(0, text.length - 2))
	}
};

rivals.loadHeroes = function($container, heroes) {
  $(heroes).each(function(i, hero) {
    $('<span>' + hero.Name + ', </span>').appendTo($container);
  });
};

rivals.loadTimeZones = function () {
	var minZone = -11, maxZone = 12;

	for (var i = minZone; i <= maxZone; i++) {
		$('#timezone').append($('<option>', { value: i, text: 'GMT' + (i != 0 ? (i > 0 ? "+" + i : i) : '') }));
	}
};

rivals.calculateCurrentRewardTimezone = function(lagCurrentReward) {
	var utcTime = moment().utc();
	utcTime = lagCurrentReward === true ? utcTime.subtract({minutes: 5}) : utcTime;
	var utcHour = parseInt(utcTime.format('H'));
	
	return rivals.timeZones[((rivals.rewardHour - utcHour) + 24) % 24];
};

rivals.shiftTimezone = function (timeZone, offset) {
	var tz = timeZone + offset;
	return rivals.timeZones[(tz + 24) % 24];
};

rivals.displayTimeline = function() {
  var $timeline = $('#cd-timeline'), $timezones = $('#all-times > div');
  
  // Local time 21:05-22:05
 	rivals.setRewards($('#reward-passed'), {hour: 0, minute: 0}, {hour: 1, minute: 0});
 	// Local time 20:05-21:05
	rivals.setRewards($('#reward-now'), {hour: -1, minute: 0}, {});
	// Local time 17:05-20:05
	rivals.setRewards($('#reward-soon'), {hour: -4, minute: -5}, {hour: -1, minute: 0});

  $timeline.show();
  $timezones.hide();
  
  $('#toggleView').text('(View All Timezones)').off('click').on('click', function() {
    rivals.displayTimeZones();
  });
}

rivals.displayTimeZones = function() {
  var $timeline = $('#cd-timeline'), $timezones = $('#all-times > div');
  
  $timeline.hide();
  $timezones.show();
  
  rivals.loadTimeZones($timezones);
  
  $('#toggleView').text('(View Upcoming Rewards)').off('click').on('click', function() {
    rivals.displayTimeline();
  });
}


rivals.loadTimeZones = function($container) {
  $(rivals.jsonTimes).each(function(i,v) {
    $('<div>Timezone ' + v.Timezone + ' : ' + v.Name + '</div>').appendTo($container);
  });
}

rivals.log = function (message) {
	if (typeof console !== 'undefined' && console && console.log && rivals.debug === true) {
		console.log(message);
	} else {
		rivals.utils.logs = rivals.utils.logs || [];
		rivals.utils.logs.push(message);
	}
};