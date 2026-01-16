import {
	type CollectionList,
	CollectionsGetListDocument,
	CollectionGetDataDocument,
} from "../../gql/graphql";
import { executeGraohql } from "./products";

export const getCollectionsList = async () => {
	const graphqlResponse = await executeGraohql(CollectionsGetListDocument, {});
	if (!graphqlResponse.collections) {
		throw new TypeError("GraphQL error: no data");
	}
	return graphqlResponse.collections.data;
};

export const getCollectionData = async (collectionName: CollectionList["data"][0]["slug"]) => {
	const graphqlResponse = await executeGraohql(CollectionGetDataDocument, {
		slug: collectionName,
	});

	if (!graphqlResponse.collection) {
		throw new TypeError("GraphQL error: no products");
	}
	return graphqlResponse.collection;
};