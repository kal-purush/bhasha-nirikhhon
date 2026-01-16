// external js: isotope.pkgd.js
 $(document).ready(function(e) {   
    // your code here

// init Isotope
var $grid = $('.grid').isotope({
  itemSelector: '.card',
  layoutMode: 'masonry',
  masonry: {
    isFitWidth: true,
    columnWidth: '.grid-sizer',
  },
  getSortData: {
    title: '.card-title',
    author: '.card-text',
    topic: '.card-topic',
    score: '.score parseInt',
  },
  sortAscending: {
    title: true,
    author: true,
    topic: true,
    score: false,
  }
});

// filter functions
var filterFns = {
  // show if number is greater than 50
  numberGreaterThan50: function() {
    var number = $(this).find('.number').text();
    return parseInt( number, 10 ) > 50;
  },
  // show if name ends with -ium
  ium: function() {
    var name = $(this).find('.name').text();
    return name.match( /ium$/ );
  }
};

// bind filter button click
$('#filters').on( 'click', 'button', function() {
  var filterValue = $( this ).attr('data-filter');
  // use filterFn if matches value
  filterValue = filterFns[ filterValue ] || filterValue;
  $grid.isotope({ filter: filterValue });
  console.log("Filtered");
});

// bind sort button click
$('#sorts').on( 'click', 'button', function() {
  var sortByValue = $(this).attr('data-sort-by');
  $grid.isotope({ sortBy: sortByValue });
  console.log("Sorted");
});

// change is-checked class on buttons
$('.button-group').each( function( i, buttonGroup ) {
  var $buttonGroup = $( buttonGroup );
  $buttonGroup.on( 'click', 'button', function() {
    $buttonGroup.find('.is-checked').removeClass('is-checked');
    $( this ).addClass('is-checked');
  });
});

console.log("isotope.js loaded");
});