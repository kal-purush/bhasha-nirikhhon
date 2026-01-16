import $ from 'jquery';
import setEvents from './setEvents.js';
import setPage from './setPage.js';

const fetchSearch = (e) => {
  e.preventDefault();
  return (dispatch, getState) => {
    const { word } = getState();
    if (word.length > 1) {
      $.ajax({
        url: `/events?q=${word}`,
        type: 'GET',
        contentType: 'application/json',
      })
        .done((res) => {
          console.log(res);
          dispatch(setEvents(res));
          dispatch(setPage(0));
        })
        .fail(() => {
          console.log('fail to get search results from db.json');
        });
    }
  };
};

export default fetchSearch;