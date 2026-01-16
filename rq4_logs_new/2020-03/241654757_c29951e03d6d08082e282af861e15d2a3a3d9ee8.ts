import algoliasearch from "algoliasearch";
import dotenv from "dotenv";

dotenv.config();

export default class AlgoliaService {
  private client: any;
  private index: any;
  constructor() {
    this.client = algoliasearch(
      process.env.ALGOLIA_CLI as string,
      process.env.ALGOLIA_PASS as string
    );
    this.index = this.client.initIndex("Posts");
  }

  async savePost(Post: any) {
    if (
      process.env.IS_TESTING === "true" ||
      !Post ||
      Post === undefined ||
      !Post.objectID
    ) {
      return Post;
    }
    try {
      if (!Post.objectID || Post.objectID === undefined) {
        throw Error("Provide objectID");
      }
      console.log(Post.objectID);
      const res = await this.index.saveObject(Post);
      console.log(res);
    } catch (error) {
      console.log(error);
      throw Error("Error with search service Create");
    }
  }

  async updatePost(Post: any) {
    if (
      process.env.IS_TESTING === "true" ||
      !Post ||
      Post === undefined ||
      !Post.objectID
    ) {
      return Post;
    }
    try {
      console.log(Post.objectID);
      const res = await this.index.partialUpdateObject(Post);
      console.log(res);
    } catch (error) {
      console.log(error);
      throw Error("Error with search service Edit");
    }
  }

  async deletePost(objectID: string) {
    if (
      process.env.IS_TESTING === "true" ||
      !objectID ||
      objectID === undefined
    ) {
      return objectID;
    }
    try {
      console.log(objectID);
      const res = await this.index.deleteObject(objectID);
      console.log(res);
    } catch (error) {
      console.log(error);
      throw Error("Error with search service Delete");
    }
  }
}