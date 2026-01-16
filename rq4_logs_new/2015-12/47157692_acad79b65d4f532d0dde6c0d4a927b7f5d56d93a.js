$(document).ready(function() {
  
var newColumn;

function buildTweeters(tweeterData){
  var colDiv = $("<div>").addClass("col-md-4");
  var imageDiv = $("<div>").addClass("img-thumbnail");
  var tweeterImage= $("<img>").attr("src", "https://media.licdn.com/mpr/mpr/shrinknp_400_400/AAEAAQAAAAAAAAYCAAAAJDI3MTYxMmI5LWQ3OGUtNDk4Zi04YjMwLWI1NmNjZGFjMTFhMw.jpg" + tweeterData.id);
  var tweeterInfoDiv = $("<div>").addClass("caption");
  var tweeterIdH4 = $("<h4>").append("the tweeter id is: " + tweeterData.id);
  var tweetCreatedH5 = $("<h5>").append("the tweet was created on: " + tweeterData.created_at);
  var tweeterFollowers =$("<p>").append(" the tweeter's number of followers: " + tweeterData.user.followers_count);
  var tweeterLocation =$("<p>").append(" the tweeter's location is: " + tweeterData.user.location)
  var tweetTimeZone = $("<p>").append("the time zone is: " + tweeterData.user.time_zone);

  imageDiv
    .append(tweeterImage)
    .append(tweeterInfoDiv
      .append(tweeterIdH4)
      .append(tweetCreatedH5)
      .append(tweeterFollowers)
      .append(tweeterLocation)
      .append(tweetTimeZone))

  colDiv.append(imageDiv);

  return colDiv;
}




for (var i = 0; i < window.users.length; i++) {
    newColumn = buildTweeters(window.users[i]);
    $(".row").append(newColumn);
  }

});