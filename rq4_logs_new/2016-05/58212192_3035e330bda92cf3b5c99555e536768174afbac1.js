$( document ).ready(function() {
$('#nextBtn').on('click', function(e) {
  e.preventDefault();
  var $current = $('.current'),
    $next = $current.nextAll('.slide').first();
  if (!$next.length) {
    $next = $('.slide').first();
  }

  if ($next.length) {
    var $nextFirst = $next.first();
    var top = $nextFirst.offset().top;

    $current.removeClass('current');

    $('.slides').animate({
      scrollTop: top
    }, function() {
      $next.addClass('current');
    });
  }
});
//****Previous Button****
$('#prevBtn').on('click', function(e) {
  e.preventDefault();
  var $current = $('.current'),
    $prev = $current.prevAll('.slide').first();
  if (!$prev.length) {
    $prev = $('.slide').first();
  }
//If there is an array
  if ($prev.length) {
    var $prevFirst = $prev.prev();
    var top = $prevFirst.offset().top;

    $current.removeClass('current');

    $('slides').animate({
      scrollTop: top
    }, function() {
      $prev.addClass('current');
    });
  }
});
});