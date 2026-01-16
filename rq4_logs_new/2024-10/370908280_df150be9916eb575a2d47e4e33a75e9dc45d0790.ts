import { sanityClient } from "lib/sanity.client";
import { componentsQuery } from "pages/api/query";
import { dynamicComponents } from "components/common";

export async function dynamicComponentsData({ type, fields, isEcommerce }) {
  const schemaData =
    (await sanityClient.fetch(componentsQuery, {
      schema: type,
    })) || [];

  return dynamicComponents({
    data: schemaData,
    schemaFields: fields,
    schemaType: type,
    isEcommerce,
  });
}