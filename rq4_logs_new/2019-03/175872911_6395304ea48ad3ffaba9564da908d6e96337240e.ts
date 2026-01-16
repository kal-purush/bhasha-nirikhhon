import algoliasearch, { Client, ClientOptions } from "algoliasearch";
import { CONFIG } from "./config";
import axios from "axios";

export const createClient = (opts?: ClientOptions): Client => {
    return algoliasearch(CONFIG.appId, CONFIG.adminKey, opts);
};

export const client = createClient();

const index = client.initIndex('lily_demo_ecommerce');

index.setSettings({
  // select the attributes that you want to be able to search in
  searchableAttributes: [
    'brand', 'name', 'categories', 'description'
  ],
  // define business metrics for ranking and sorting
  customRanking: [
    'desc(popularity)'
  ],
  // set up some attributes to filter results on
  attributesForFaceting: [
    'categories', 'searchable(brand)', 'price'
  ]
})

axios.get('https://alg.li/doc-ecommerce.json')
  .then((response) => {
    return response;
  })
  .then((products: any) => {
    // replace the 'any' type here
    index.addObjects(products.data);
  })
  .catch((err) => {
    console.log(err);
  })