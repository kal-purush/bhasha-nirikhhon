'use strict';

export function burgerShowAndHide(button, container, title) {
    if (!checkClass('body-overflow-hidden')) {
        button.classList.remove('header__burger-button--inactive');
        button.classList.add('header__burger-button--active');
        container.classList.remove('header__burger-menu-container--inactive');
        container.classList.add('header__burger-menu-container--active');
        document.body.classList.add('body-overflow-hidden');
        title.classList.add('header__logo-link--invisible');
    } else if (checkClass('body-overflow-hidden')) {
        button.classList.add('header__burger-button--inactive');
        button.classList.remove('header__burger-button--active');
        container.classList.add('header__burger-menu-container--inactive');
        container.classList.remove('header__burger-menu-container--active');
        document.body.classList.remove('body-overflow-hidden');
        title.classList.remove('header__logo-link--invisible');
    }
}

export function hideBurgerWhenClickGrayArea(event, button, container, title, linkCollection) {
    for (let item of linkCollection) {
        if (event.target === container && event.target !== item) {
            button.classList.remove('header__burger-button--active');
            button.classList.add('header__burger-button--inactive');
            container.classList.remove('header__burger-menu-container--active');
            document.body.classList.remove('body-overflow-hidden');
            title.classList.remove('header__logo-link--invisible');
            container.classList.add('header__burger-menu-container--inactive');
        }
    }
}

export function checkClass(className) {
    return document.body.classList.contains(className);
}

/**
 * Generates a random number (0 >= petsCollection.length - 1 < petsCollection.length).
 */
export function random(multiplier) {
    return Math.floor(Math.random() * multiplier);
}

/**
 * Creates a layout of cards on page load.
 */
export function createPetsLayout(objCollection, multiplier, container) {
    const fragment = document.createDocumentFragment(),
        line = [];

    while (line.length != multiplier) {
        let randomNum = random(multiplier);

        if (!line.includes(randomNum)) {
            line.push(randomNum);
        }
    }

    line.forEach(num => fragment.append(createPetCard(objCollection[num])));

    container.append(fragment);
}

/**
 * Create Pet card (DOM element)
 * @param {*} pet - Pet Object
 * @returns DOM Object
 */
export function createPetCard(pet) {
    const petCard = document.createElement('div');
    petCard.classList.add('our-friends-card', 'pets-pet-card', 'pet-card');
    petCard.setAttribute('id', `${pet.name.toLowerCase()}-card`);

    const petCardFigure = document.createElement('figure');
    petCardFigure.classList.add('our-friends-card-pic', 'pet-card-pic');
    petCard.append(petCardFigure);

    const petCardFigureImgContainer = document.createElement('div');
    petCardFigureImgContainer.classList.add('pet-card-pic-img-container');
    petCardFigure.append(petCardFigureImgContainer);

    const petCardFigureImg = document.createElement('img');
    petCardFigureImg.classList.add('pet-card-pic-img');
    petCardFigureImg.setAttribute('src', pet.img);
    petCardFigureImg.setAttribute('alt', pet.name);
    petCardFigureImgContainer.append(petCardFigureImg);

    const petCardFigureCaption = document.createElement('figcaption');
    petCardFigureCaption.classList.add('our-friends-card-name', 'pet-card-name');
    petCardFigureCaption.innerHTML = pet.name;
    petCardFigure.append(petCardFigureCaption);

    const petCardInformationBtn = document.createElement('a');
    petCardInformationBtn.classList.add('our-friends-card-link', 'link-btn-transparent', 'pet-card-link');
    petCardInformationBtn.setAttribute('href', '#!');
    petCardInformationBtn.innerHTML = 'Learn more';
    petCard.append(petCardInformationBtn);

    const petCardPopupContainer = document.createElement('div');
    petCardPopupContainer.classList.add('our-friends-card-popup-container', 'pet-card-popup-container');
    petCard.append(petCardPopupContainer);

    const petCardPopup = document.createElement('div');
    petCardPopup.classList.add('our-friends-card-popup', 'pet-card-popup');
    petCardPopupContainer.append(petCardPopup);

    const petCardPopupImgContainer = document.createElement('div');
    petCardPopupImgContainer.classList.add('our-friends-card-popup-img-container', 'pet-card-popup-img-container');
    petCardPopup.append(petCardPopupImgContainer);

    const petCardPopupImg = document.createElement('img');
    petCardPopupImg.classList.add('our-friends-card-popup-img', 'pet-card-popup-img');
    petCardPopupImg.setAttribute('src', pet['img-big']);
    petCardPopupImg.setAttribute('alt', pet.name);
    petCardPopupImgContainer.append(petCardPopupImg);

    const petCardPopupDescrContainer = document.createElement('div');
    petCardPopupDescrContainer.classList.add('our-friends-card-popup-descr-container', 'pet-card-popup-descr-container');
    petCardPopup.append(petCardPopupDescrContainer);

    const petCardPopupTitle = document.createElement('h3');
    petCardPopupTitle.classList.add('our-friends-card-popup-title', 'pet-card-popup-title', 'h3-title');
    petCardPopupTitle.innerHTML = pet.name;
    petCardPopupDescrContainer.append(petCardPopupTitle);

    const petCardPopupType = document.createElement('h4');
    petCardPopupType.classList.add('our-friends-card-popup-type', 'pet-card-popup-type', 'h4-title');
    petCardPopupType.innerHTML = `${pet.type} - ${pet.breed}`;
    petCardPopupDescrContainer.append(petCardPopupType);

    const petCardPopupDecr = document.createElement('p');
    petCardPopupDecr.classList.add('our-friends-card-popup-descr', 'pet-card-popup-descr');
    petCardPopupDecr.innerHTML = pet.description;
    petCardPopupDescrContainer.append(petCardPopupDecr);

    const petCardPopupAge = document.createElement('p');
    petCardPopupAge.classList.add('our-friends-card-popup-age', 'pet-card-popup-age', 'pet-card-popup-property');
    petCardPopupAge.innerHTML = `<b>Age: </b>${pet.age}`;
    petCardPopupDescrContainer.append(petCardPopupAge);

    const petCardPopupIno = document.createElement('p');
    petCardPopupIno.classList.add('our-friends-card-popup-ino', 'pet-card-popup-ino', 'pet-card-popup-property');
    petCardPopupIno.innerHTML = `<b>Inoculations: </b>${pet.inoculations}`;
    petCardPopupDescrContainer.append(petCardPopupIno);

    const petCardPopupDis = document.createElement('p');
    petCardPopupDis.classList.add('our-friends-card-popup-diseases', 'pet-card-popup-diseases', 'pet-card-popup-property');
    petCardPopupDis.innerHTML = `<b>Diseases: </b>${pet.diseases}`;
    petCardPopupDescrContainer.append(petCardPopupDis);

    const petCardPopupPar = document.createElement('p');
    petCardPopupPar.classList.add('our-friends-card-popup-parasites', 'pet-card-popup-parasites', 'pet-card-popup-property');
    petCardPopupPar.innerHTML = `<b>Parasites: </b>${pet.parasites}`;
    petCardPopupDescrContainer.append(petCardPopupPar);

    const petCardPopupCloseBtn = document.createElement('button');
    petCardPopupCloseBtn.classList.add('pet-card-popup-close-btn');
    petCardPopupCloseBtn.setAttribute('type', 'button');
    petCardPopup.append(petCardPopupCloseBtn);

    return petCard;
}

/**
 * Checks the number of the current page. If it's the first/second page, the function disables prevPage(firstPage)/*nextPage(lastPage) the buttons.
 */
export function checkPageNumber(lastNum) {
    const buttonNextPage = document.getElementById('button-next-page'),
    buttonLastPage = document.getElementById('button-last-page'),
    buttonPrevPage = document.getElementById('button-prev-page'),
    buttonFirstPage = document.getElementById('button-first-page'),
    pageNumElem = document.querySelector('.pets-our-friends-page-circle');
    if (+pageNumElem.innerHTML === 1) {
        buttonPrevPage.classList.remove('btn-transparent');
        buttonFirstPage.classList.remove('btn-transparent');
        buttonNextPage.classList.remove('btn-inactive');
        buttonLastPage.classList.remove('btn-inactive');
        buttonPrevPage.classList.add('btn-inactive');
        buttonFirstPage.classList.add('btn-inactive');
        buttonNextPage.classList.add('btn-transparent');
        buttonLastPage.classList.add('btn-transparent');
        buttonPrevPage.disabled = true;
        buttonFirstPage.disabled = true;
        buttonNextPage.disabled = false;
        buttonLastPage.disabled = false;
    } else if (+pageNumElem.innerHTML === lastNum) {
        buttonNextPage.classList.remove('btn-transparent');
        buttonLastPage.classList.remove('btn-transparent');
        buttonPrevPage.classList.remove('btn-inactive');
        buttonFirstPage.classList.remove('btn-inactive');
        buttonNextPage.classList.add('btn-inactive');
        buttonLastPage.classList.add('btn-inactive');
        buttonPrevPage.classList.add('btn-transparent');
        buttonFirstPage.classList.add('btn-transparent');
        buttonNextPage.disabled = true;
        buttonLastPage.disabled = true;
        buttonPrevPage.disabled = false;
        buttonFirstPage.disabled = false;
    } else {
        buttonPrevPage.classList.remove('btn-inactive');
        buttonFirstPage.classList.remove('btn-inactive');
        buttonNextPage.classList.remove('btn-inactive');
        buttonLastPage.classList.remove('btn-inactive');
        buttonNextPage.classList.add('btn-transparent');
        buttonLastPage.classList.add('btn-transparent');
        buttonPrevPage.classList.add('btn-transparent');
        buttonFirstPage.classList.add('btn-transparent');
        buttonNextPage.disabled = false;
        buttonLastPage.disabled = false;
        buttonPrevPage.disabled = false;
        buttonFirstPage.disabled = false;
    }
}