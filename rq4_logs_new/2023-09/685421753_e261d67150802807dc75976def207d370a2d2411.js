let repos = [];
let lastIndex = 0;

async function fetchGitHubRepos() {
    const response = await fetch('https://api.github.com/users/juliarodriguesamaral/repos');
    repos = await response.json();
    displayRepos();
}

function displayRepos() {
    const projectsDiv = document.getElementById('projetos');
    const nextRepos = repos.slice(lastIndex, lastIndex + 6);

    nextRepos.forEach((repo) => {
        const card = `
            <div class="card rounded shadow-lg p-4 w-full sm:bg-white sm:text-black md:bg-white md:text-black" id="card1">
                <h2 class="text-xl font-bold card-title">${repo.name}</h2>
                <i class="fab fa-${repo.language ? repo.language.toLowerCase() : 'code'} card-icon"></i>
                <p class="text-sm mt-2 card-description">${repo.description || 'Sem descrição'}</p>
            </div>
        `;
        projectsDiv.innerHTML += card;
    });

    lastIndex += 6;

    if (lastIndex >= repos.length) {
        document.getElementById('loadMore').disabled = true;
    }
}

document.getElementById('loadMore').addEventListener('click', displayRepos);

fetchGitHubRepos();