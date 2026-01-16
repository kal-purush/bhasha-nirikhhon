const btnRandom = document.querySelector("#btnRandom");
// const tailRandom = document.querySelector("#tailRandom");
// const template = document.querySelector("#template").content;
// const fragment = document.createDocumentFragment();
const btnPrep = document.querySelector(".btnPrep");
const modal = document.querySelector(".modal");
const closeModal = document.querySelector(".closeModal");
const imageRandom = document.querySelector(".imageRandom");
const nameRandom = document.querySelector(".nameRandom");
const ingrRandom = document.querySelector(".ingrRandom");
const instRandom = document.querySelector(".instRandom");
const glassRandom = document.querySelector(".glassRandom");
const catRandom = document.querySelector(".catRandom");

const connectApi = async () => {
    try {
        const response = await fetch("https://www.thecocktaildb.com/api/json/v1/1/random.php");
        const data = await response.json();

        pintarCocktail(data);
        // check(data);
    } catch (error) {
        console.error("Error ====> ", error);
    }
};

// const check = (data) => {
//     const princ = data.drinks[0];
//     Object.values(princ).forEach((item) => console.log(item));
// };

const pintarCocktail = (data) => {
    const princ = data.drinks[0];
    imageRandom.src = princ.strDrinkThumb;
    imageRandom.alt = princ.strDrink;
    nameRandom.textContent = princ.strDrink;
    instRandom.textContent += " " + princ.strInstructions;
    glassRandom.textContent += " " + princ.strGlass;
    catRandom.textContent += " " + princ.strCategory;

    for (let i = 1; i <= 15; i++) {
        const measure = princ["strMeasure" + i];
        const ingredient = princ["strIngredient" + i];

        if (measure || ingredient) {
            ingrRandom.textContent += ` ${measure || ""} ${ingredient || ""} - `;
        }
    }
};

document.addEventListener("DOMContentLoaded", () => {
    connectApi();
});

btnRandom.addEventListener("click", () => {
    connectApi();
});

btnPrep.addEventListener("click", () => {
    modal.classList.add("openModal");
});

closeModal.addEventListener("click", () => {
    modal.classList.remove("openModal");
});

// const pintarCocktail = (data) => {
//     console.log(data.drinks[0]);

//     const clone = template.cloneNode(true);
//     clone.querySelector(".imageRandom").src = data.drinks[0].strDrinkThumb;
//     clone.querySelector(".imageRandom").alt = data.drinks[0].strDrink;
//     clone.querySelector(".nameRandom").textContent = data.drinks[0].strDrink;
//     clone.querySelector(".instRandom").textContent = data.drinks[0].strInstructions;
//     clone.querySelector(
//         ".ingrRandom"
//     ).textContent = `${data.drinks[0].strMeasure1} ${data.drinks[0].strIngredient1} - ${data.drinks[0].strMeasure2} ${data.drinks[0].strIngredient2} - ${data.drinks[0].strMeasure3} ${data.drinks[0].strIngredient3} - ${data.drinks[0].strMeasure4} ${data.drinks[0].strIngredient4} - ${data.drinks[0].strMeasure5} ${data.drinks[0].strIngredient5} - ${data.drinks[0].strIngredient6}`;
//     clone.querySelector(".glassRandom").textContent = data.drinks[0].strGlass;
//     clone.querySelector(".catRandom").textContent = data.drinks[0].strCategory;

//     fragment.appendChild(clone);
//     tailRandom.appendChild(fragment);
// };