const VITE_URL="https://api.spoonacular.com/recipes/complexSearch"
const VITE_APIKEY="aa25a52a460c44fdbf53f25bd5746789"

export default async function fetchFood(query) {
    const res = await fetch(
      `${VITE_URL}?query=${query}&apiKey=${
        VITE_APIKEY
      }`
    );
    const data = await res.json();
    return data.results;
  }

(async()=>{
    console.log(await fetchFood("pizza"))

})()
 console.log( fetchFood("pizza"))