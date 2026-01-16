const form = document.querySelector('form');
const tweetList = document.querySelector('.tweet-list');

form.addEventListener('submit', saveTweet);

function saveTweet(e) {
  e.preventDefault();
  const tweetText = document.querySelector('#tweet-text').value;

  let tweets = localStorage.getItem('tweets')
    ? JSON.parse(localStorage.getItem('tweets'))
    : [];

  const tweet = {
    text: tweetText,
    time: new Date().toLocaleString(),
  };

  tweets.unshift(tweet);
  localStorage.setItem('tweets', JSON.stringify(tweets));
  displayTweets();
}

function displayTweets() {
  const tweets = localStorage.getItem('tweets')
    ? JSON.parse(localStorage.getItem('tweets'))
    : [];

  tweetList.innerHTML = '';

  tweets.forEach((tweet) => {
    const li = document.createElement('li');
    li.classList.add('tweet');
    li.innerHTML = `
      <img src="https://picsum.photos/50" alt="avatar">
      <div>
        <span class="username">User:</span>
        <p class="tweet-text">${tweet.text}</p>
        <div class="tweet-actions">
          <i class="fa fa-retweet"></i>
          <span>0</span>
          <i class="fa fa-heart"></i>
          <span>0</span>
          <i class="fa fa-ellipsis-h"></i>
        </div>
        <p class="tweet-time">${tweet.time}</p>
      </div>
    `;
    tweetList.appendChild(li);
  });
}

displayTweets();