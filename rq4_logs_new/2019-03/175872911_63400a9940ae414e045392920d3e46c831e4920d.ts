import axios from "axios";
import { client } from "./create-client";
import algoliasearchHelper from "algoliasearch-helper";

export const search = async (params: any) => {
  const helper = algoliasearchHelper(client, "lily_demo_ecommerce", {
    facets: ["categories", "searchable(brand)", "price"],
  });
  const response = await helper.searchOnce();
  console.log(response);
  // return mapResponse(response.content);
};

export const insert = async () => {
  const index = client.initIndex("lily_demo_ecommerce");

  // index.setSettings({
  //   // select the attributes that you want to be able to search in
  //   searchableAttributes: [
  //     "brand", "name", "categories", "description"
  //   ],
  //   // define business metrics for ranking and sorting
  //   customRanking: [
  //     "desc(popularity)",
  //   ],
  //   // set up some attributes to filter results on
  //   attributesForFaceting: [
  //     "categories", "searchable(brand)", "price",
  //   ],
  // });

  axios.get("https://alg.li/doc-ecommerce.json")
    .then((response) => {
      return response;
    })
    .then((products: any) => {
      // replace the 'any' type here
      index.addObjects(products.data);
    })
    .catch((err) => {
      console.log(err);
    });
};

