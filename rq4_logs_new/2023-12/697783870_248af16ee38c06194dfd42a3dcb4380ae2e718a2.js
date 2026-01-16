export function fetchAllProducts() {
  return new Promise(async (resolve) =>{
    //TODO: we wil not hardcode the server URL here
    const response = await(fetch("http://localhost:8080/products"));
    const data = await response.json();
    resolve(data);
  }
  );
}

export function fetchProductsByFilter(filter) {

  let queryString="";

  for(let key in filter){
    queryString += `${key}=${filter[key]}&`
  }

  return new Promise(async (resolve) => {
    //TODO: we wil not hardcode the server URL here
    const response = await fetch("http://localhost:8080/products?"+queryString);
    const data = await response.json();
    resolve(data);
  });
}