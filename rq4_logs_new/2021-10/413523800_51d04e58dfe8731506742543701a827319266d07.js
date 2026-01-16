let articleDiv = document.querySelector("div");

window.addEventListener("load", loadArticles);

function loadArticles () {
    axios.get("http://localhost:4400/api/articles")
    .then((res) => {
      for (let i =0; i <= 9; i++) {
        let {source, author, title, description, url, publishedAt} = res.data[i];
        let articleInfo = {
          source: source.name,
          author: author,
          title: title,
          description: description,
          url: url,
          date: publishedAt
        }
        // add title
        let articleTitle = document.createElement("ul");
        articleTitle.textContent = articleInfo.title;
        articleDiv.appendChild(articleTitle);

        // add source
        let articleSource = document.createElement("li");
        articleSource.textContent = articleInfo.source;
        articleTitle.appendChild(articleSource);

        // add author
        let articleAuthor = document.createElement("li");
        articleAuthor.textContent = articleInfo.author;
        articleTitle.appendChild(articleAuthor);

        //add date
        let articleDate = document.createElement("li");
        articleDate.textContent = articleInfo.date;
        articleTitle.appendChild(articleDate);

        //add description
        let articleDesc = document.createElement("li");
        articleDesc.textContent = articleInfo.description;
        articleTitle.appendChild(articleDesc);

        //add URL
        let articleURL = document.createElement("li");
        articleURL.textContent = articleInfo.url;
        articleTitle.appendChild(articleURL);

        //add image later?
    };
  })
};