import { getDefaultHeaders } from "./utils";

export async function addProduct(product_url) {
  let headers = getDefaultHeaders();
  const data = {
    url: product_url,
  };

  const requestOptions = {
    method: "POST",
    body: JSON.stringify(data),
    headers: headers,
  };
  const url = "http://localhost:5000/product_details";
  try {
    const response = await fetch(url, requestOptions);
    const jsonResponse = await response.json();
    console.log("Response: ", jsonResponse);
    return jsonResponse;
  } catch (err) {
    console.log("Response: error");
    return [];
  }
}