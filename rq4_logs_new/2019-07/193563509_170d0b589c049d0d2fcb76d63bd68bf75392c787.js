import { elements } from "./base";

export const cleartour = () => {
    elements.tour.innerHTML = "";
};

const createTour = tour => `
        <li class="tour__package">
                <p>
                    <span><b>Inicia: </b></span>${tour.fechaInicio}</p>
                <p>
                    <span><b>Termina: </b></span>${tour.fechaFin}
                </p>
                <p>
                    ${tour.asientosReservados} asientos reservados / ${tour.asientosDisponibles} disponibles 
                </p>
                <p>
                    US $${tour.precioRealUSD} por persona
                </p>
                    
                <button class="btn-small tour__btn tour__btn--add">
                    <svg class="search__icon">
                        <use href="img/icons.svg#icon-shopping-cart"></use>
                    </svg>
                <span>Reservar</span>
            </button>
        </li>
        <hr>
`;

export const rendertour = (tour) => {
    const markup = `
        <figure class="tour__fig">
            <img src="${tour.img}" alt="${tour.title}" class="tour__img">
            <h1 class="tour__title">
                <span>${tour.nombreTour}</span>
            </h1>
        </figure>
        <div class="tour__details">
            <div class="tour__info">
                <span class="tour__info-data tour__info-data--minutes">Desde US $${tour.precioUSD}</span>
            </div>
        </div>

        <div class="tour__details">
            <div class="tour__info" style="text-align: center;">
                <span class="tour__info-data tour__info-data--minutes">
                <p>Salimos desde:</p>
                ${tour.zonaSalida}
                </span>
            </div>
            <div class="tour__info" style="text-align: center;">
            <span class="tour__info-data tour__info-data--minutes">
            <p>Llegamos a:</p>
                ${tour.zonaLlegada}
            </span>
            </div>
        </div>

        <div class="tour__details">
            <div class="tour__info" style="text-align: center;">
                <span class="tour__info-data tour__info-data--minutes">
                    <p>${tour.descripcion}</p>
                    <p>Escalas: ${tour.escalas.map(el => el).join(" > ")}</p>
                </span>
            </div>
        </div>

        <div class="tour__ingredients">
        <ul class="tour__ingredient-list">
            <h2>¿Qué incluye?</h2>
            <li class="tour__item" style="">
                <svg class="tour__icon">
                    <use href="img/icons.svg#icon-check"></use>
                </svg>
                <div class="tour__ingredient">
                        <span class="tour__unit">Acomodación: ${tour.acomodacion  ? "Incluida" : "No incluida"}</span>
                    </div>
                </li>  
                <li class="tour__item">
                    <svg class="tour__icon">
                        <use href="img/icons.svg#icon-check"></use>
                    </svg>
                    <div class="tour__ingredient">
                        <span class="tour__unit">Transporte: ${tour.transporte ? "Incluido" : "No Incluido"}</span>
                    </div>
                </li>

                <li class="tour__item">
                    <svg class="tour__icon">
                        <use href="img/icons.svg#icon-check"></use>
                    </svg>
                    <div class="tour__ingredient">
                        <span class="tour__unit">${tour.duracionTour} ${tour.duracionTour === 1 ? "Día" : "Días"} de Viaje</span>
                    </div>
                </li>
        
                <li class="tour__item">
                    <svg class="tour__icon">
                        <use href="img/icons.svg#icon-check"></use>
                    </svg>
                    <div class="tour__ingredient">
                        <span class="tour__unit">${tour.tipoDeTour}</span>
                    </div>
                </li>
        </ul>
        <ul class="tour__ingredient-list">
                        <h2>Requisitos</h2>
                        <li class="tour__item">
                        <svg class="tour__icon">
                            <use href="img/icons.svg#icon-check"></use>
                        </svg>
                        <div class="tour__ingredient">
                        <span class="tour__unit">Edad Mínima: ${tour.edadMinima}</span>
                    </div>
                </li>
                        <li class="tour__item">
                            <svg class="tour__icon">
                                <use href="img/icons.svg#icon-check"></use>
                            </svg>
                            <div class="tour__ingredient">
                                <span class="tour__unit">Seguro de Viaje: ${tour.requisitos.seguroDeViaje ? "Incluido" : "No incluido"}</span>   
                            </div>
                        </li>
                    </ul>
                </div>        


        <div class="tour__directions">
            <h2 class="heading-2">Próximos Tours</h2>
            <div class="tour__info" style="margin-bottom: 3em;">
                <svg class="tour__info-icon">
                    <use href="img/icons.svg#icon-man"></use>
                </svg>
                <span class="tour__info-data tour__info-data--people">${tour.servings}</span>
                <span class="tour__info-text"> personas</span>
                <div class="tour__info-buttons">
                    <button class="btn-tiny btn-decrease">
                        <svg>
                            <use href="img/icons.svg#icon-circle-with-minus"></use>
                        </svg>
                    </button>
                    <button class="btn-tiny btn-increase">
                        <svg>
                            <use href="img/icons.svg#icon-circle-with-plus"></use>
                        </svg>
                    </button>
                </div>
            </div>
            <ul>
                ${tour.fechasDisponibles["2019"].map(tour => createTour(tour))}
            </ul>
        </div>
    `;
  elements.tour.insertAdjacentHTML("afterbegin", markup);
};

export const updateServingsIngredients = tour => {

document.querySelector(".tour__info-data--people").textContent =
    tour.servings;

const countElements = Array.from(document.querySelectorAll(".tour__count"));
    countElements.forEach((el, i) => {
        el.textContent = formatCount(tour.ingredients[i].count);
    });
};

