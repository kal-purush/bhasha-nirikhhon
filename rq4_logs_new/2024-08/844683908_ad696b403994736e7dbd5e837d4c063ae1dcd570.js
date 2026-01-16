document.addEventListener('mousemove', function(event) {
    let container = document.getElementById('container');

    // Array original de GIFs com os caminhos fornecidos
    const gifs = [
        'images/Agave.gif',
        'images/Big_Fern.gif',
        'images/Bitter-Smack_Leaf.gif',
        'images/Blackened_Flower.gif',
        'images/Bitter-Smack_Leaf.gif',
        'images/Bog_Fingers_Plant.gif',
        'images/Chill_Nettle.gif',
        'images/Ferns.gif',
        'images/Ficus_Benjamina.gif',
        'images/Jungle_Umbrella_Plant_(Large).gif',
        'images/Kelp.gif',
        'images/Laminaria.gif',
        'images/Large_Coral.gif',
        'images/Mire_Sprout.gif',
        'images/Monkey_Tail.gif',
        'images/Mud_Whip.gif',
        'images/Pillar_Growth.gif',
        'images/Pillar_Vine.gif',
        'images/Putrid_Plant.gif',
        'images/Sequoia_Trunk.gif',
        'images/Shadow_Plant.gif',
        'images/Sludge_Fern.gif',
        'images/Small_Fern.gif',
        'images/Swamp_Tentacles.gif',
        'images/Tower_Fern.gif'
    ];

    // Função para selecionar aleatoriamente 3 imagens do array gifs
    function getRandomImages(array, num) {
        const shuffled = array.slice().sort(() => 0.5 - Math.random());
        return shuffled.slice(0, num);
    }

    // Seleciona aleatoriamente 3 imagens
    const selectedGifs = getRandomImages(gifs, 3);

    // Seleciona um GIF aleatório das 3 imagens selecionadas
    let gifIndex = Math.floor(Math.random() * selectedGifs.length);

    // Cria um novo elemento de imagem
    let img = document.createElement('img');
    img.src = selectedGifs[gifIndex]; // Atribui o GIF selecionado
    img.classList.add('plant');

    // Define a posição da imagem baseada na posição do mouse
    img.style.left = event.pageX + 'px';
    img.style.top = event.pageY + 'px';

    // Adiciona a imagem ao container
    container.appendChild(img);

    // As imagens permanecem visíveis, não são removidas
});