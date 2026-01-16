/*In questo esercizio, utilizzerai async/await per creare la funzione getChefBirthday(id). Questa funzione accetta un id di una ricetta e deve:
Recuperare la ricetta da https://dummyjson.com/recipes/{id}
Estrarre la proprietà userId dalla ricetta
Usare userId per ottenere le informazioni dello chef da https://dummyjson.com/users/{userId}
Restituire la data di nascita dello chef
Note del docente:
Scrivi la funzione getChefBirthday(id), che deve:
Essere asincrona (async).
Utilizzare await per chiamare le API.
Restituire una Promise con la data di nascita dello chef.
Gestire gli errori con try/catch*/

//funzione di supporto per il parse dei json
async function fetchData(url) {
  const res = await fetch(url);
  const obj = res.json();
  return obj;
}

const getChefBirthday = async (id) => {
  const recipe = await fetchData(`https://dummyjson.com/recipes/${id}`);
  const chef = await fetchData(`https://dummyjson.com/users/${recipe.userId}`);

  return chef.birthDate;
};

getChefBirthday(1)
  .then((birthday) => console.log("Data di nascita dello chef:", birthday))
  .catch((error) => console.error("Errore:", error.message));