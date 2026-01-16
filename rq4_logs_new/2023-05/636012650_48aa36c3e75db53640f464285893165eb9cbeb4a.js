//Obtém a lista de favoritos do Local Storage ou cria uma nova lista caso esteja vazia.
const favorites = JSON.parse(localStorage.getItem('favorites')) || [];

const btnFavorites = document.querySelectorAll(".btn-favorite");

//Carrega os favoritos ao carregar a página.
window.addEventListener('load', function () {
  updateFavoritesList();
})


//Função que adiciona o produto na lista de favoritos
function addToFavorites(button) {

  //váriaveis que reebem o valor dos cards
  const card = button.parentNode;
  const name = card.querySelector('h5').innerText
  const image = card.querySelector('img').src;
  const description = card.querySelector('.card-text').innerText;
  const price = card.querySelector('span').innerText;
  const id = button.dataset.id;

  //json com as informações do produto
  const data = {
    name: name,
    image: image,
    description: description,
    price: price,
    id: id
  };

  //Adiciona os dados na constante favoritos 
  favorites.push(data);

  //adiciona os dados no Local Storage
  localStorage.setItem('favorites', JSON.stringify(favorites));
  
  //Atualiza a lista de favoritos no site
  updateFavoritesList();
}


//Função para atualizar a lista de favoritos
function updateFavoritesList() {

  //Retorna a quantidade de produtos adicionados aos favoritos
  document.querySelector('#total-favorites').textContent = favorites.length;

  const badge = document.querySelectorAll(".badge").forEach((value) => {
    favorites.length > 0 ?  value.textContent = favorites.length : value.textContent= '';
  });
  
  
  
  //Condicional para exibir a lista de itens somente quando a listagem de favoritos não estiver vazia
  if (favorites.length > 0) {

    let favoritesHTML = '';

    //A cada iteração será criado um card de produto que será acrescentado na váriavel favoritesHTML
    favorites.forEach(function (favorite) {
      const favoritesCard = document.createElement('div');
      favoritesCard.innerHTML = `<div class="modal-favorites">
    <div class="modal-img">
      <img src="${favorite.image}" alt="imagem do ${favorite.name}">
    </div>
    <div class="modal-info">
      <i onclick="toggleFavorite(${favorite.id})"  class="fa-regular fa-trash-can"></i>
      <h5 class="card-title">${favorite.name}</h5>
      <p> ${favorite.description} </p>
      
      <div class="footer-info">
          <div class="price">
            Por R$: <span> ${favorite.price} </span>
          </div>
          <div class="button">
              <button class=" btn btn-primary"> Comprar </button>
          </div>
      </div>
    </div>
  </div>`;

      favoritesHTML += favoritesCard.innerHTML;
    });

    //Atualiza a lista de favoritos
    const favoritesList = document.getElementById('favorites-list');
    favoritesList.innerHTML = favoritesHTML;

  }
  else {

    //Adiciona uma imagem informando que a lista está vazia.
    const favoritesList = document.getElementById('favorites-list');
    const favoritesEmpty = `<div class="modal-favorites">
      <img class="image-empty" src="assets/img/empty_cart.png" alt="lista vazia">
    </div>`;

    //Adiciona a imagem no corpo do modal
    favoritesList.innerHTML = favoritesEmpty;
  }


}


//Função para remover o produto da lista de favoritos
//Recebe como parametro o botão que foi clicado , no caso o icon trash
function removeFromFavorites(button) {
  
  //Obtém o elemento pai no caso o card com toda informação do produto
  const card = button.parentNode.parentNode;
  //Obtém o nome pelo elemento h5 dentro das inforamçãoes recebidas
  const name = card.querySelector('h5').innerText;

  //Pesquisa o índice do item no array "favorites", que é armazenado no local storage do navegador, usando a função findIndex().
  const index = favorites.findIndex(function (favorite) {
    return favorite.name === name;
  });

  //Se o item for encontrado, ele é removido do array "favorites" usando a função splice().
  if (index !== -1) {
    favorites.splice(index, 1);

    //Atualiza o array favoritos no Local Storage e no Site removendo o produto da listagem.
    localStorage.setItem('favorites', JSON.stringify(favorites));
    updateFavoritesList();
  }
}


const favoritesList = document.getElementById('favorites-list'); 

// Adiciona o manipulador de eventos para o botão de remover
favoritesList.addEventListener('click', function (event) {

  //aramazena a referência do elemento clicado
  const target = event.target;

  //Se o botão clicado tiver a classe que informa que ele é o ícone de lixeira chama a função que remove o produto da listagem.
  if (target.classList.contains('fa-trash-can')) {
    removeFromFavorites(target);
  }
});



//Para cada botão de favorito no card ele altera a classe do icone e adicona/remove dos favoritos
btnFavorites.forEach(function(btnFavorite) {
  btnFavorite.addEventListener("click", function () {
    const iconFavorite = btnFavorite.querySelector(".icon-favorite");

    if(iconFavorite.classList.contains("fa-regular")){
      iconFavorite.classList.add('fa-solid');
      iconFavorite.classList.remove('fa-regular');
      addToFavorites(this);
    }else{
      iconFavorite.classList.add('fa-regular');
      iconFavorite.classList.remove('fa-solid');
      removeFromFavorites(this);
    }
  });
});


//Ao carregar a página ele verifica os itens que estão na lista de favoritos para deixa o ícone marcado do respectivo card
document.addEventListener("DOMContentLoaded", () => {

  document.querySelectorAll(".btn-favorite").forEach((button) => {
    const id = button.getAttribute("data-id");
  
    // Verifica se o card está presente na lista de favoritos
    const index = favorites.findIndex((favorite) => favorite.id === id);
    if (index !== -1) {
      button.querySelector("i.icon-favorite").classList.add("fa-solid");
      button.querySelector("i.icon-favorite").classList.remove("fa-regular");
    } else {
      button.querySelector("i.icon-favorite").classList.remove("fa-solid");
      button.querySelector("i.icon-favorite").classList.add("fa-regular");
    }
  });

});



//Função desmarca o botão de favorito quando ele é removido pelo botão de excluir dentro do modal de favoritos
function toggleFavorite(id) {

  const favoriteButton = document.querySelector(`[data-id="${id}"]`);
  if (favoriteButton) {
    const iconFavorito = favoriteButton.querySelector('.icon-favorite');
    iconFavorito.classList.remove('fa-solid');
    iconFavorito.classList.add('fa-regular');

  }
}










