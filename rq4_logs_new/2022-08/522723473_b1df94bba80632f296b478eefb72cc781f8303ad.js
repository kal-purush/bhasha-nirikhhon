// fetch old to dos from the local storage
let events =
  window.localStorage.getItem('events') ||
  JSON.stringify({
    9: '',
    10: '',
    11: '',
    12: '',
    13: '',
    14: '',
    15: '',
    16: '',
    17: '',
  });
events = JSON.parse(events);

$(document).ready(function () {
  $('#currentDay').text(moment().format('dddd, MMMM Do,   h:mm:ss a'));

  function timeColor() {
    var taskHour = parseInt(moment().format('H'));
    // for each array method that loops through all the scheduled time to get the active todo and set its color
    $('.hour').each(function () {
      // setting old to do value
      $(this)[0].children[1].children[0].value =
        events[$(this).attr('data-id')];

      var rightNow = parseInt($(this).attr('data-id'));
      //   setting current to do color
      if (rightNow === taskHour) {
        setColor($(this)[0], '#ff5f5f'); // red
      } else if (rightNow > taskHour) {
        // setting future to do color
        setColor($(this)[0], '#67e967'); // green
      } else {
        //  setting past to do color
        setColor($(this)[0], '#c5bcbc'); // gray
      }
    });
  }
  timeColor();

  //   color setter function
  function setColor(element, color) {
    element.style.backgroundColor = color;
  }

  // save to do function
  $('.saveBtn').each(function () {
    $(this).click(function () {
      let btn = $(this); // save btn with click function
      $('.hour').each(function () {
        let todo = '';
        //check which todo corresponds to the clicked button using their data-id
        if (btn.attr('data-id') === $(this).attr('data-id')) {
          todo = $(this)[0].children[1].children[0].value;
          // update the specific event that had a to do change.
          events = { ...events, [$(this).attr('data-id')]: todo };
        }
      });
      //   set the updated event back to the local storage
      window.localStorage.setItem('events', JSON.stringify(events));
    });
  });
});