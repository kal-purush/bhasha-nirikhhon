async function getData () {
    try {
        const response = await fetch('https://api.jikan.moe/v4/anime');
        const data = await response.json();
        const animeList = document.getElementById('anime-list');
        if (!response.ok) {
            throw new Error(`Error: ${response.status}`);
        }
        data.data.forEach(anime => {
            const animeItem = document.createElement('div');
            animeItem.classList.add('anime-item');

            const animeImage = document.createElement('img');
            animeImage.src = anime.images.jpg.image_url;
            animeImage.alt = anime.title;

            const animeTitle = document.createElement('h2');
            animeTitle.textContent = anime.title;

            const animeSynopsis = document.createElement('p');
            animeSynopsis.textContent = anime.synopsis ? anime.synopsis : 'No synopsis available.';

            animeItem.appendChild(animeImage);
            animeItem.appendChild(animeTitle);
            animeItem.appendChild(animeSynopsis);
            animeList.appendChild(animeItem);
        });
    } catch (error) {
        console.error('Error fetching data:', error);
    }
}
getData();