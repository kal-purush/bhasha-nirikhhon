var run_search = function() {
  var user_search_term = $("#search_term").val();
  $.ajax({
    type: "POST",
    url: 'http://en.wikipedia.org/w/api.php?',
    dataType: 'jsonp',
    data: {
      'action': 'query',
      'format': 'json',
      'async': false,
      'gsrsearch': user_search_term,
      'generator': 'search',
      'prop': 'info'
    },
    headers: {
      'Api-User-Agent': 'Example/1.0'
    },
    xhrFields: {
      withCredentials: true
    },
    success: function(data) {
      var results = data.query.pages;
      console.log("succeeded");
      /*console.log(results);*/
      $("#holder").html("<h2>Wikipedia Results: " + user_search_term + "</h2>");
      if ($("#results_container").text()) {
        console.log("the div is empty");
      }
      $.each(results, function(index, value) {
        var container = document.createElement('div');
        container.appendChild(document.createTextNode(index + ": " + value.title));
        console.log(index + ": " + value.title);
        document.getElementById("results_container").appendChild(container);
      });
    },
    error: function() {
      console.log("erred")
    }

  });
}