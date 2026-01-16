const input = document.getElementById("input");
const autocompleteList = document.getElementById("autocomplete-list");
const repoList = document.getElementById("list");
let addedRepos = [];

const debounce = (fn, delay) => {
  let timeoutId;
  return function (...args) {
    clearTimeout(timeoutId);
    timeoutId = setTimeout(() => {
      fn.apply(this, args);
    }, delay);
  };
};

const fetchRepositories = async (query) => {
  if (!query || query.trim() === "") {
    autocompleteList.replaceChildren();
    return;
  }

  try {
    const response = await fetch(
      `https://api.github.com/search/repositories?q=${query}&per_page=5`
    );
    const data = await response.json();
    displaySuggestions(data.items);
  } catch (error) {
    console.error("Ошибка:", error);
  }
};

const displaySuggestions = (repos) => {
  autocompleteList.replaceChildren();
  repos.forEach((repo) => {
    const suggestion = document.createElement("div");
    suggestion.className = "suggestion";
    suggestion.textContent = repo.name;
    suggestion.onclick = () => addRepository(repo);
    autocompleteList.appendChild(suggestion);
  });
};

const addRepository = (repo) => {
  if (!addedRepos.some((r) => r.id === repo.id)) {
    addedRepos.push(repo);
    displayRepoList();
  }
  input.value = "";
  autocompleteList.replaceChildren();
};

const displayRepoList = () => {
  repoList.replaceChildren();
  addedRepos.forEach((repo) => {
    const repoItem = document.createElement("div");
    repoItem.className = "repo-item";
    repoItem.insertAdjacentHTML(
      "afterbegin",
      `
            <div>Name: ${repo.name}<br>
            Owner: ${repo.owner.login}<br>
            Stars: ${repo.stargazers_count}<br></div>
            <button class="remove-btn" onclick="removeRepository(${repo.id})">Удалить</button>
        `
    );
    repoList.appendChild(repoItem);
  });
};

const removeRepository = (repoId) => {
  addedRepos = addedRepos.filter((repo) => repo.id !== repoId);
  displayRepoList();
};

input.addEventListener(
  "input",
  debounce(() => {
    fetchRepositories(input.value);
  }, 400)
);