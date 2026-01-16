$(document).ready(function () {
  $('.saveBtn').on('click', function () {

    var text = $(this).siblings('.description').val();
    var time = $(this).parent().attr('id');

    localStorage.setItem(time, text);
    
    $('.feedback').addClass('show');

   setTimeout(function () {
      $('.feedback').removeClass('show');
    }, 2500);
  });
  
    function updateHour() {
      var currentHour = dayjs().hour();

      $('.time-block').each(function() {
      var setHour =parseInt($(this).attr('id').split('-')[1]);

      if (setHour === currentHour) {
        $(this).addClass('present');
      } else if (setHour < currentHour) {
        $(this).removeClass('present');
        $(this).addClass('past');
      } else {
        $(this).removeClass('past');
        $(this).removeClass('present');
        $(this).addClass('future');
      }
    });
  }
  
  updateHour();

  setInterval(updateHour, 5000);

  $('#today').text(dayjs().format('dddd, MMMM D, YYYY'));

  [8,9,10,11,12,13,14,15,16,17].forEach(hour => {
    $(`#hour-${hour} .description`).val(localStorage.getItem(`hour-${hour}`));
  });
  
});
