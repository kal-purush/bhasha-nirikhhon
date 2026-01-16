var posts=["2025/04/09/当我听米店的时候，我在听些什么/"];function toRandomPost(){
    pjax.loadUrl('/'+posts[Math.floor(Math.random() * posts.length)]);
  };