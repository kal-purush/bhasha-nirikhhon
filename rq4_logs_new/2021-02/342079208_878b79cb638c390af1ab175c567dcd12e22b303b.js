// NOTA: Dividir as funcionalidades de JS conforme o modelo MVC
let DOMStrings = {
  // LOGIN
  eyeIcon: '.icon-eye-style',
  passwordInput: '.password-type',
  // TABELAS
  cardDecklist: document.querySelector('#deck-list'),
  thead: document.querySelector('thead'),
  editIconsList: '.editIcon',
  cardCount: document.querySelectorAll('#cardCount')[0],
  // INPUTS DA TELA DE DECKS
  deckName: document.querySelector('#deckname'),
  deckAtt1: document.querySelector('#deckattribute1'),
  deckAtt2: document.querySelector('#deckattribute2'),
  deckAtt3: document.querySelector('#deckattribute3'),
  deckAtt4: document.querySelector('#deckattribute4'),
  deckPhotoFileInput: document.querySelector('#deckImage'),
  deckPhotolbl: document.querySelector('#deckImagelbl'),
  deckImageView: document.querySelector('#deckImageView'),
  // INPUTS DA TELA DE CARDS
  cardName: document.querySelector('#cardname'),
  cardAtt1: document.querySelector('#cardattribute1'),
  cardAtt2: document.querySelector('#cardattribute2'),
  cardAtt3: document.querySelector('#cardattribute3'),
  cardAtt4: document.querySelector('#cardattribute4'),
  specialAttName: document.querySelector('#specialAttributeName'),
  specialAttRef: document.querySelector('#specialAttributeRef'),
  specialAttValue: document.querySelector('#specialAttributeValue'),
  cardPhotolbl: document.querySelector('#cardImagelbl'),
  cardPhotoFileInput: document.querySelector('#cardImage'),
  cardImageView: document.querySelector('#cardImageView'),
  // BOTÕES
  newDeckbtn: document.querySelector('#btn-new-deck'),
  newCardbtn: document.querySelector('#btn-new-card'),
  deleteDeckbtn: document.querySelector('#btn-delete-deck'),
  saveDeckbtn: document.querySelector('#btn-save-deck'),
  deleteCardbtn: document.querySelector('#btn-delete-card'),
  saveCardbtn: document.querySelector('#btn-save-card'),
  addCardbtn: document.querySelector('#btn-add-card'),
  closeDeckbtn: document.querySelector('#btn-close-deck'),
  // SELECT DECK HOME
  selectDeckHome: document.querySelector('#deckHomeImage'),
}

// Função troca visibilidade da senha
function changePasswordIndex() {
  let eye = document.querySelector('.icon-eye-style').getAttribute('src');
  let password = document.querySelector('.password-type').type;

  if (eye === "./public/img/icons/eye.svg" && password === "password") {
    document.querySelector('.icon-eye-style').src = "./public/img/icons/hide-eye.svg";
    document.querySelector('.password-type').type = "text";
  } else {
    document.querySelector('.icon-eye-style').src = "./public/img/icons/eye.svg";
    document.querySelector('.password-type').type = "password";
  };
};

function changePasswordPrivate() {
  let eye = document.querySelector('.icon-eye-style').getAttribute('src');
  let password = document.querySelector('.password-type').type;

  if (eye === "../../public/img/icons/eye.svg" && password === "password") {
    document.querySelector('.icon-eye-style').src = "../../public/img/icons/hide-eye.svg";
    document.querySelector('.password-type').type = "text";
  } else {
    document.querySelector('.icon-eye-style').src = "../../public/img/icons/eye.svg";
    document.querySelector('.password-type').type = "password";
  };
};

function changePassword() {
  let eye = document.querySelector('.icon-eye-style').getAttribute('src');
  let password = document.querySelector('.password-type').type;

  if (eye === "../img/icons/eye.svg" && password === "password") {
    document.querySelector('.icon-eye-style').src = "../img/icons/hide-eye.svg";
    document.querySelector('.password-type').type = "text";
  } else {
    document.querySelector('.icon-eye-style').src = "../img/icons/eye.svg";
    document.querySelector('.password-type').type = "password";
  };
};

// NOTA: Transformar em função
DOMStrings.cardDecklist.addEventListener('scroll', function () {
  let translate = "translate(0,' + this.scrollTop + 'px)";
  console.log(translate);
  DOMStrings.thead.style.transform = translate;
});

// NOTA: Transformar em função - Para cada elemento do tipo "ícone de edição", adicionar uma função ao clique que redireciona para a mesma página, porém setando o método GET com o ID do item clicado
document.querySelectorAll('.editIcon').forEach(element => {
  element.addEventListener('click', function () {
    if (window.location.href.includes('create-deck')) {
      window.location.href = "/Mega-Triunfo/Projeto/public/html/create-deck.php?idForEdit=" + parseInt(element.getAttribute('name'));
    } else if (window.location.href.includes('create-card')) {
      if (window.location.href.includes('&idForEdit=')) {
        window.location.href = window.location.href.split('&idForEdit')[0] + "&idForEdit=" + parseInt(element.getAttribute('name'));
        console.log(window.location.href);
      } else {
        window.location.href = "/Mega-Triunfo/Projeto/public/html/create-card.php" + window.location.href.split('php')[1] + "&idForEdit=" + parseInt(element.getAttribute('name'));
      }
    }
  });
});


if (DOMStrings.cardCount) {
  DOMStrings.cardCount.textContent = `Cartas (${document.querySelector('.noselect').children.length})`;
}

// NOTA: Transformar em função - Ao clique que "Novo baralho", reseta a página
if (DOMStrings.newDeckbtn) {
  DOMStrings.newDeckbtn.addEventListener('click', function () {
    window.location.href = "/Mega-Triunfo/Projeto/public/html/create-deck.php";
  })
};

if (DOMStrings.newCardbtn) {
  DOMStrings.newCardbtn.addEventListener('click', function () {
    let get = window.location.href.split('?')[1];
    window.location.href = "/Mega-Triunfo/Projeto/public/html/create-card.php?" + get.split('&')[0];
  })
};

// NOTA: Transformar em função - Ao clique de "Adicionar carta", vai para a próxima página
if (DOMStrings.addCardbtn) {
  DOMStrings.addCardbtn.addEventListener('click', function () {
    if (window.location.href.includes('?idForEdit=')) {
      window.location.href = "/Mega-Triunfo/Projeto/public/html/create-card.php?deckIdForEdit=" + window.location.href.split('=')[1];
    } else {
      alert('Você precisa selecionar um baralho para gerenciar suas cartas!')
    }
  })
};

// NOTA: Transformar em função - Ao clique de "Fechar baralho", retorna ao menu anterior
if (DOMStrings.closeDeckbtn) {
  DOMStrings.closeDeckbtn.addEventListener('click', function () {
    window.location.href = "/Mega-Triunfo/Projeto/public/html/create-deck.php";
  })
};


// NOTA: Disparar função apenas quando os campos do form estiverem preenchidos. Alert para salvamento (adição ou edição) de novas cartas e baralhos
function saveButton() {
  if (document.querySelector('.savebtn').id == 'btn-save-card') {
    if (window.location.href.includes('?idForEdit=')) {
      alert('Carta alterada!');
    } else {
      alert('Carta salva!')
    }
  } else {
    if (window.location.href.includes('?idForEdit=')) {
      alert('Baralho alterado!');
    } else {
      alert('Baralho salvo!')
    }
  }
};

// NOTA: Disparar função apenas quando os campos do form estiverem preenchidos. - Alert para confirmação de exclusão de baralho
function deleteButton() {
  if (document.querySelector('.deletebtn').id == 'btn-delete-card') {
    if (window.location.href.includes('idForEdit=')) {
      confirm('Deseja deletar a carta? Não é possível recuperá-la.');
      alert('Carta deletada!');
    } else {
      alert('É necessário selecionar uma carta para deletar.')
    }
  } else {
    if (window.location.href.includes('idForEdit=')) {
      confirm('Deseja deletar o baralho e suas cartas? Não é possível recuperá-los.');
      alert('Baralho deletado!');
    } else {
      alert('É necessário selecionar um baralho para deletar.')
    }
  }
};

// NOTA: Transformar em função - Alterando o label e a prévia da imagem quando uma imagem for carregada
if (document.querySelector('.custom-file-input')) {
  document.querySelector('.custom-file-input').addEventListener('change', function () {
    if (this.files[0].size > 5242880) {
      alert('Tamanho máximo excedido (5 MB).');
      this.value = '';
    }
    if (this === DOMStrings.deckPhotoFileInput) {
      DOMStrings.deckPhotolbl.textContent = this.value;
    } else {
      DOMStrings.cardPhotolbl.textContent = this.value;
    }
    readURL(this);
  })
};

function readURL(input) {
  if (input.files && input.files[0]) {
    var reader = new FileReader();
    reader.onload = function (e) {
      if (input === DOMStrings.deckPhotoFileInput) {
        DOMStrings.deckImageView.src = e.target.result;
      } else {
        DOMStrings.cardImageView.src = e.target.result;
      }
    }
    reader.readAsDataURL(input.files[0]);
  }
}

// Função para atualizar os campos do item selecionado com o array que veio da consulta PHP
function updateInputsForEdit() {
  if (window.$PHPEditArray && window.location.href.includes('create-deck')) {

    DOMStrings.deckName.value = $PHPEditArray[0]['Name'];
    DOMStrings.deckAtt1.value = $PHPEditArray[0]['Attribute1'];
    DOMStrings.deckAtt2.value = $PHPEditArray[0]['Attribute2'];
    DOMStrings.deckAtt3.value = $PHPEditArray[0]['Attribute3'];
    DOMStrings.deckAtt4.value = $PHPEditArray[0]['Attribute4'];
    DOMStrings.deckImageView.src = $PHPEditArray[0]['Photo'];

  } else if (window.$PHPEditCardArray && window.location.href.includes('create-card')) {
    DOMStrings.cardName.value = $PHPEditCardArray[0]['Name'];
    DOMStrings.cardAtt1.value = $PHPEditCardArray[0]['Attribute1'];
    DOMStrings.cardAtt2.value = $PHPEditCardArray[0]['Attribute2'];
    DOMStrings.cardAtt3.value = $PHPEditCardArray[0]['Attribute3'];
    DOMStrings.cardAtt4.value = $PHPEditCardArray[0]['Attribute4'];
    DOMStrings.specialAttName.value = $PHPEditCardArray[0]['Special Name'];
    DOMStrings.specialAttValue.value = $PHPEditCardArray[0]['Special Value'];
    DOMStrings.specialAttRef.value = $PHPEditCardArray[0]['Special Ref'];
    DOMStrings.cardImageView.src = $PHPEditCardArray[0]['Photo'];
  }
}

updateInputsForEdit();

function isNumber(e) {
  // Atribuindo qual o representante da tecla na tabela ASCII ou, se não encontrar, qual valor representa no teclado
  var charCode = (e.which) ? e.which : e.keyCode
  if (charCode > 31 && (charCode < 48 || charCode > 57))
    return false;
  return true;
}

// DECK LIST HOME - Pegando ID do baralho selecionado
function selectDeckHome(id) {
  let idDeckCard = id.id.split('Id')[1];
  let imageUrl = id.src.split('decks/')[1];
  //DOMStrings.selectDeckHome.src = imageSRC;
  window.location.href = `/Mega-Triunfo/Projeto/public/html/start-game.php?deckId=${idDeckCard}&deckUrl=${imageUrl}`;
  //window.location.href = `/Mega-Triunfo/Projeto/private/php/cardSelectHome.php?deckId=${idDeckCard}`;
}