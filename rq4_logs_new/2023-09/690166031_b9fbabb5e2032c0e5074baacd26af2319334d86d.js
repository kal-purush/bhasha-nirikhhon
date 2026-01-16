function loadData() {
  fetch("https://jsonplaceholder.typicode.com/users")
    .then((res) => res.json())
    .then((data) => loadName(data));
}

function loadName(users) {
  const userName = document.getElementById("userName");
  userName.innerHTML = "";
  let count = 0;
  for (const name of users) {
    count += 1;
    // console.log(name.name)
    const p = document.createElement("p");
    p.innerHTML = ` <p class="text-xl text-white p-2">${count}. ${name.name} </p>`;
    userName.appendChild(p);
  }
}
function loadData2() {
  fetch("https://jsonplaceholder.typicode.com/users")
    .then((res) => res.json())
    .then((data) => loadUserName(data));
}

function loadUserName(users) {
  let count = 0;
  const userUserName = document.getElementById("userUserName");
  userUserName.innerHTML = "";
  for (const name of users) {
    count += 1;
    // console.log(name.name)
    const p = document.createElement("p");
    p.innerHTML = ` <p class="text-xl text-white p-2">${count}. ${name.username} </p>`;
    userUserName.appendChild(p);
  }
}

function loadPostsData() {
  fetch("https://jsonplaceholder.typicode.com/posts")
    .then((res) => res.json())
    .then((data) => loadPosts(data));
}

function loadPosts(contents) {
  const postContainer = document.getElementById("postContainer");
  postContainer.innerHTML = "";
  for (let content of contents) {
    console.log(content);
    const div = document.createElement("div");
    div.innerHTML = `<div class="card card-compact  bg-base-100 shadow-2xl">
        <div class="card-body">
          <h2 class="card-title">${content.title}</h2>
          <p>ID:${content.id}</p>
          <p>${content.body}</p>

        </div>
      </div>`;
      div.classList.add('my-5')
      postContainer.appendChild(div);
  }
}
