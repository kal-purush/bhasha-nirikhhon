const searchParams = new URLSearchParams(window.location.search);
const articleId = searchParams.get("article");

fetch(
  "https://kea2s21-6ee9.restdb.io/rest/posts/" +
    articleId +
    "?fetchchildren=true",
  {
    method: "GET",
    headers: {
      "x-apikey": "602e2bac5ad3610fb5bb629b",
    },
  }
)
  .then((res) => res.json())
  .then((response) => {
    showPost(response);
  })
  .catch((err) => {
    console.error(err);
  });

function showPost(data) {
  console.log(data.comments);
  document.querySelector("h1").textContent = data.title;
  document.querySelector("h4 span").textContent = data.username;
  document.querySelector("p").textContent = data.content;

  //data.comments would be the aray

  //grab the template content

  const template = document.querySelector(".commentTemplate").content;

  // loop through data.comments
  data.comments.forEach((comment) => {
    console.log(comment);

    // create a clone
    const clone = template.cloneNode(true);

    // populate/change content
    clone.querySelector(".commentContent").textContent = comment.content;

    clone.querySelector(".commentUsername").textContent = comment.username;

    // append it

    document.querySelector(".commentList").appendChild(clone);
  });
  if (data.comments.length === 0) {
    const clone = template.cloneNode(true);

    clone.querySelector(".panel .commentContent").textContent =
      "No comments yet, be the first!";

    clone.querySelector(".panel .commentUsername").textContent = "you";
    document.querySelector(".commentList").appendChild(clone);
  }
}

const form = document.querySelector("#commentForm");
// evl
form.addEventListener("submit", handleSubmit);

//grab the form data
function handleSubmit(e) {
  e.preventDefault();
  console.log(form.elements);
  const payload = {
    username: form.elements.username.value,
    email: form.elements.email.value,
    content: form.elements.content.value,
    date: Date.now(),
  };

  console.log(payload);
  //send a request
  fetch(`https://kea2s21-6ee9.restdb.io/rest/posts/${articleId}/comments`, {
    method: "POST",
    headers: {
      "x-apikey": "602e2bac5ad3610fb5bb629b",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  })
    .then((res) => res.json())
    .then((comment) => {
      //grab the template content

      const template = document.querySelector(".commentTemplate").content;
      // create a clone
      const clone = template.cloneNode(true);

      // populate/change content
      clone.querySelector(".commentContent").textContent = comment.content;

      clone.querySelector(".commentUsername").textContent = comment.username;

      // append it

      document.querySelector(".commentList").appendChild(clone);
    });
}

//handle response