const userCards = document.getElementById("userCards");
const addPostBtn = document.getElementById("addPostBtn");
const newTitle = document.getElementById("newTitle");
const newBody = document.getElementById("newBody");

let nextId = 101; // ID counter for added posts

const fetchData = async () => {
  try {
    const response = await fetch("https://jsonplaceholder.typicode.com/posts");
    const posts = await response.json();
    posts.slice(0, 13).forEach((post) => {
      createCard(post);
    });
  } catch (err) {
    console.error("Error fetching data:", err);
  }
};

function createCard(post) {
  const card = document.createElement("div");
  card.classList.add("card");

  const idHeading = document.createElement("h2");
  idHeading.textContent = `ID: ${post.id}`;

  const title = document.createElement("p");
  title.textContent = post.title;

  const body = document.createElement("p");
  body.textContent = post.body;

  const btnGroup = document.createElement("div");
  btnGroup.classList.add("btn-group");

  const editBtn = document.createElement("button");
  editBtn.textContent = "Edit";
  editBtn.className = "edit-btn";

  const deleteBtn = document.createElement("button");
  deleteBtn.textContent = "Delete";
  deleteBtn.className = "delete-btn";

  editBtn.addEventListener("click", () => {
    const titleInput = document.createElement("input");
    titleInput.value = post.title;

    const bodyInput = document.createElement("textarea");
    bodyInput.value = post.body;

    const saveBtn = document.createElement("button");
    saveBtn.textContent = "Save";
    saveBtn.className = "save-btn";

    card.replaceChild(titleInput, title);
    card.replaceChild(bodyInput, body);
    btnGroup.replaceChild(saveBtn, editBtn);

    saveBtn.addEventListener("click", () => {
      post.title = titleInput.value;
      post.body = bodyInput.value;

      title.textContent = post.title;
      body.textContent = post.body;

      card.replaceChild(title, titleInput);
      card.replaceChild(body, bodyInput);
      btnGroup.replaceChild(editBtn, saveBtn);
    });
  });

  deleteBtn.addEventListener("click", () => {
    card.remove();
  });

  btnGroup.appendChild(editBtn);
  btnGroup.appendChild(deleteBtn);

  card.appendChild(idHeading);
  card.appendChild(title);
  card.appendChild(body);
  card.appendChild(btnGroup);

  userCards.appendChild(card);
}

// Add new post
addPostBtn.addEventListener("click", () => {
  const title = newTitle.value.trim();
  const body = newBody.value.trim();

  if (title && body) {
    const newPost = {
      id: nextId++,
      title,
      body,
    };
    createCard(newPost);
    newTitle.value = "";
    newBody.value = "";
  } else {
    alert("Please fill in both title and body.");
  }
});

fetchData();