alert("hello")
$(document).ready(function() {
  $.ajax({
    type: "GET",
    url: "https://api.github.com/users/Twofirst21/repos?sort=pushed",
    success: function(repos) {
      for(var i = 0; i < repos.length; i++) {
        var newListItem = buildListGroup(repos[i]);
        $(".list-group").append(newListItem);
      }
    },
    error: function(jqXHR, textStatus, errorThrown) {
      alert("Something went wrong. We are looking into it!");
    }
  });

  function buildListGroup(repoData) {
    var commitsApiUrl = "https://api.github.com/repos/";
    commitsApiUrl += repoData.owner.login + "/";
    commitsApiUrl += repoData.name + "/commits";
    
    var newLink = $("<a>")
      .attr("href", commitsApiUrl)
      .addClass("list-group-item")
      .append(repoData.full_name);
    return newLink;
  }
});