var H5P = H5P || {};

H5P.TwitterUserFeed = (function ($) {
  /**
   * Constructor function.
   */
  function C(options, id) {
    // Extend defaults with provided options
    this.options = $.extend(true, {}, {
      userName: 'H5PTechnology',
      showReplies: false,
      numTweets: 5
    }, options);
    // Keep provided id.
    this.id = id;
  };

  /**
   * Attach function called by H5P framework to insert H5P content into
   * page
   *
   * @param {jQuery} $container
   */
  C.prototype.attach = function ($container) {
    
    this.setUpTwitter();

    // Set class on container to identify it as a greeting card
    // container.  Allows for styling later.
    $container.addClass("h5p-twitter-feed");

    // Add greeting text.
    $container.append(
      '<a class="twitter-timeline" href="https://twitter.com/twitterapi"\\n\
      data-widget-id="558756407995273216" data-screen-name="' + this.options.userName
      + '" data-show-replies="' + this.options.showReplies 
      + '" data-tweet-limit="' + this.options.numTweets + '">Tweets by @'
      + this.options.userName + '</a>');
  };
  
  C.prototype.setUpTwitter = function() {
    window.twttr = (function(d, s, id) {
      var js, fjs = d.getElementsByTagName(s)[0],
        t = window.twttr || {};
      if (d.getElementById(id)) return;
      js = d.createElement(s);
      js.id = id;
      js.src = "https://platform.twitter.com/widgets.js";
      fjs.parentNode.insertBefore(js, fjs);

      t._e = [];
      t.ready = function(f) {
        t._e.push(f);
      };

      return t;
    }(document, "script", "twitter-wjs"));
  }

  return C;
})(H5P.jQuery);