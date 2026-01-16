const URL = 'https://swapi-api.hbtn.io/api/films/?format=json';

$.getJSON(URL, (data) => {
  const results = data.results;
  $.each(results, (key, value) => {
    $('#list_movies').append('<li>' + value.title + '</li>');
  });
});