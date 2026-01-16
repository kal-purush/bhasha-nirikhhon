const reloadAndClearForm = function() {
  loadTweets();
  emptyTextArea('#tweet-text');
  $('.counter').text(140);
}

const loadTweets = function() {
  $.ajax('/tweets/', { method: 'GET' })
  .then(function (tweets) {
    const sortedTweets = sortTweetsByCreationDate(tweets);
    renderTweets(sortedTweets);
  });
};

const emptyTextArea = textarea => $(textarea).val("");
//const clearCharCount = counter => counter.text();
const sortTweetsByCreationDate = tweetArray => tweetArray.sort((a, b) => b["created_at"] - a["created_at"]);

const renderTweets = tweetArray => {
  $('.all-tweets-container').empty();
  const tweets = $('<div>').addClass("all-tweets");
  for (const tweet of tweetArray) {
    const tweetArticle = createTweetElement(tweet);
    tweets.append(tweetArticle);
  }
  $('.all-tweets-container').append(tweets);
}

const createTweetElement = tweet => {
  const header = `<header class="tweet__article-header">
                    <div class="tweet__user">
                      <span class="tweet__avatar">
                        <img src="${tweet.user.avatars}" width="40px" />
                      </span>
                      <span class="tweet__name">${tweet.user.name}</span>
                    </div>
                    <span class="tweet__handle">${tweet.user.handle}</span>
                  </header>`;

  const body = $('<div>').addClass("tweet__body").text(tweet.content.text);

  // calculate the amount of time since the creation date
  const dateDiff = timeSinceDate(tweet['created_at']);

  // create the footer
  const footer = `<footer class="tweet__article-footer">
                    <span class="tweet__creation-date">${dateDiff}</span>
                    <div class="tweet__buttons">
                      <i class="fas fa-flag"></i>
                      <i class="fas fa-retweet"></i>
                      <i class="fas fa-heart"></i>
                    </div>
                  </footer>`;

  const article = $('<article>').addClass("tweet").append(header).append(body).append(footer);
  return article;
};

const timeSinceDate = dateInMS => {
  let timeBetween;
  const currentDate = new Date();
  const dateDiffInMS = currentDate - dateInMS;
  const dateDiffInYears = Math.floor(dateDiffInMS / (1000 * 60 * 60 * 24 * 365));
  const dateDiffInMonths = Math.floor(dateDiffInMS / (1000 * 60 * 60 * 24 * (365 / 12)));
  const dateDiffInDays = Math.floor(dateDiffInMS / (1000 * 60 * 60 * 24));
  const dateDiffInHours = Math.floor(dateDiffInMS / (1000 * 60 * 60));
  const dateDiffInMinutes = Math.floor(dateDiffInMS / (1000 * 60));
  if (dateDiffInYears > 0) {
    timeBetween = dateDiffInYears + " year(s) ago";
  } else if (dateDiffInMonths > 0) {
    timeBetween = dateDiffInMonths + " month(s) ago";
  } else if (dateDiffInDays > 0) {
    timeBetween = dateDiffInDays + " day(s) ago";
  } else if (dateDiffInHours > 0) {
    timeBetween = dateDiffInHours + " hour(s) ago";
  } else if (dateDiffInMinutes >= 0) {
    timeBetween = dateDiffInMinutes + " minute(s) ago";
  } else {
    timeBetween = "";
  }
  return timeBetween;
}