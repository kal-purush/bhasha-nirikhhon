(function goToConferenceTimeBlock(_now) {
  var now = _now || moment();

  var dayOne = moment('2015-10-28');
  var dayTwo = moment('2015-10-29');

  var dayPrefix;
  if (now.isSame(dayOne, 'day')) {
    dayPrefix = 'DayOne_';
  } else if (now.isSame(dayTwo, 'day')) {
    dayPrefix = 'DayTwo_';
  } else {
    return; //exit if not a conference day
  }

  var bestStart;
  var nowTime = parseInt(now.format('HHmm'), 10);
  $('.time-block').each(function () {
    var blockDayStart = $(this).attr('id').split('_');
    var blockDay = blockDayStart[0];
    var blockStart = blockDayStart[1];
    if (blockDay + '_' !== dayPrefix) {
      //only look at times for current day
      return;
    }
    if (!bestStart) {
      bestStart = blockStart;
    }
    if (nowTime >= parseInt(blockStart, 10)) {
      bestStart = blockStart;
    }
  });
  window.location.hash = '#' + dayPrefix + bestStart;
})();


$(function() {
  var STARS_KEY = 'forum.agenda_stars';
  var CHECK_KEY = 'forum.star_check';

  //check if localStorage is supported by browser
  if (!$.support.localStorage) {
    $('.personalize-agenda').hide();
    $('.star-btn').hide();
    return; //return early
  }

  if (!$.localStorage(STARS_KEY)) {
    $.localStorage(STARS_KEY, {});
  }

  function setStar(id) {
    var starred = $.localStorage(STARS_KEY);
    starred[id] = true;
    $.localStorage(STARS_KEY, starred);
  }

  function removeStar(id) {
    var starred = $.localStorage(STARS_KEY);
    delete starred[id];
    $.localStorage(STARS_KEY, starred);
  }

  function checkHasAnyStars() {
    var hasAnyStars = $('.agenda-track-item.starred').length > 0;
    if (hasAnyStars) {
      $('.agenda-track').addClass('has-stars');
      $('.show-starred-check').prop('disabled', false);
    }
    else {
      $('.agenda-track').removeClass('has-stars');
      $('.show-starred-check').prop('disabled', true)
        .prop('checked', false);
      $.localStorage(CHECK_KEY, false);
      showUnstarred();
    }
  }

  function hideUnstarred() {
    $('.agenda-track-item:not(.starred)').hide();
  }

  function showUnstarred() {
    $('.agenda-track-item').show();
  }

  function loadStars() {
    var starred = $.localStorage(STARS_KEY);
    if (!starred) { return; }

    $('.agenda-track-item').each(function () {
      if (starred[this.id]) {
        $(this).addClass('starred');
      }
    });

    if ($.localStorage(CHECK_KEY)) {
      $('.show-starred-check').prop('checked', true);
      hideUnstarred();
    }

    checkHasAnyStars();
  }

  function attachStarBehavior() {
    var $showStarredCheck = $('.show-starred-check');

    $('button.star-btn').on('click', function () {
      var agendaTrackItem = $(this).parents('.agenda-track-item')[0];
      var $agendaTrackItem = $(agendaTrackItem);
      if (!agendaTrackItem) { return; }

      $agendaTrackItem.toggleClass('starred');

      if ($agendaTrackItem.hasClass('starred')) {
        setStar(agendaTrackItem.id);
      }
      else {
        removeStar(agendaTrackItem.id);
        if ($showStarredCheck.prop('checked')) {
          $agendaTrackItem.hide();
        }
      }

      checkHasAnyStars();
    });

    $showStarredCheck.on('click', function () {
      var checked = $(this).prop('checked');

      if (checked) {
        hideUnstarred();
      }
      else {
        showUnstarred();
      }

      $.localStorage(CHECK_KEY, checked);
    });
  }

  loadStars();
  attachStarBehavior();
});