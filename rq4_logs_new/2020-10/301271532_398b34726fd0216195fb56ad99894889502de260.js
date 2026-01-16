  // Helps escape possible XSS attepmts
  const escape = (str) => {
    let div = document.createElement("div");
    div.appendChild(document.createTextNode(str));
    return div.innerHTML
   }

   const createTweetElement = function(tweet) {
    let $tweet = $(`
    <article>
    <header>
      <div>
        <p>
          <img src="${tweet.user.avatars}">
          <span>${tweet.user.name}</span>
        </p> 
        <span class="reveal">${tweet.user.handle}</span> 
      </div>
    </header>
    <div class="user-tweet">
      <p>${escape(tweet.content.text)}</p>
    </div>
    <footer>
      <div>
        <p>${new Date(tweet.created_at).toLocaleString()}</p>
        <p class="icons">
          <i class="fas fa-flag"></i> 
          <i class="fas fa-retweet"></i> 
          <i class="fas fa-heart"></i>
        </p>
      </div>
    </footer>
  </article>
  `)
  return $tweet
  } 

  const renderTweets = function(tweets) {
    for (const item of tweets) {
      const result = createTweetElement(item)
      $(".tweets").prepend(result)
    }
  }

  const loadTweets = function() {
    $.ajax("/tweets", {method: "GET"})
    .then((res) => {
    $(".tweets").empty()
    renderTweets(res)
    })
  }

  const showError = function(message) {
    $("#error").empty()
    $("#error").addClass("warning")
    $("#error").append(message).hide().slideDown(500)
  }