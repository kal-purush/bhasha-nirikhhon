import axios from "axios";
import { client } from "./create-client";
import { ProductHit } from "../models";
import algoliasearchHelper from "algoliasearch-helper";

export const search = async (params: any): Promise<any> => {
// add a ProductQuery type for the above params
  const helper = await algoliasearchHelper(client, "lily_demo_ecommerce");
  const result = await helper.searchOnce();
  return result;
};

export const insert = async (): Promise<void> => {
  const index = client.initIndex("lily_demo_ecommerce");

  index.setSettings({
    // select the attributes that you want to be able to search in
    searchableAttributes: [
      "brand", "name", "categories", "description"
    ],
    // define business metrics for ranking and sorting
    customRanking: [
      "desc(popularity)",
    ],
    // set up some attributes to filter results on
    attributesForFaceting: [
      "categories", "searchable(brand)", "price",
    ],
  });

  axios.get<ProductHit[]>("https://alg.li/doc-ecommerce.json")
    .then((response) => {
      // tslint:disable-next-line: no-console
      return response;
    })
    .then((products: any) => {
      // replace the 'any' type here
      index.addObjects(products.data);
    })
    .catch((err) => {
      // tslint:disable-next-line: no-console
      console.log(err);
    });
};

