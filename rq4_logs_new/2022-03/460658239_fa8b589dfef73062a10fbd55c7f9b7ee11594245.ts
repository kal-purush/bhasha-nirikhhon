import { connect, disconnect } from "mongoose";

import { migrateDb } from "../src/migrateDb";
import env from "../src/env";

import query from "../src/resolvers/query";
import mutation from "../src/resolvers/mutation";

const user = {
  name: "Linus Bolls",
  email: "linus.bolls@code.berlin",
  permsInt: 127,
  rentingLimit: 10,
};
describe("App", () => {
  test("MongoDb Connects", async () => {
    await connect(env.MONGO_CONNECTION_STRING);
    await migrateDb();
  });
  describe("GraphQl Resolvers", () => {
    describe("Queries", () => {
      describe("getShelf", () => {
        test("it contains books", async () => {
          const res = await query.getShelf(null, null, { user });

          expect(res.children.length).toBeGreaterThan(0);
        });
      });
    });
    describe("Mutations", () => {
      let createdBookId: string;
      describe("createBook", () => {
        test("it works", async () => {
          const res = await mutation.createBook(
            null,
            {
              bookData: {
                tags: ["book"],
                name: "The kickin' Adventures of Tomas Amüsant",
                media: {
                  contentTags: ["tomas amüsant", "adventure", "life lessons"],
                  creators: ["tomas amüsant", "ghost rider"],
                  publisher: "penguin books",
                  language: "english",
                },
              },
            },
            { user }
          );
          createdBookId = res.id;
          expect(res.__typename).toEqual("Success");
        });
      });
      describe("rentBook", () => {
        test("it works", async () => {
          const res = await mutation.rentBook(
            null,
            { bookId: createdBookId },
            { user }
          );
          expect(res.__typename).toEqual("Success");
        });
      });
      describe("returnBook", () => {
        test("it works", async () => {
          const res = await mutation.returnBook(
            null,
            { bookId: createdBookId },
            { user }
          );
          expect(res.__typename).toEqual("Success");
        });
      });
      describe("processBook", () => {
        test("it works", async () => {
          const res = await mutation.processBook(
            null,
            { bookId: createdBookId },
            { user }
          );
          expect(res.__typename).toEqual("Success");
        });
      });
    });
  });
  test("MongoDb Disconnects", async () => {
    await disconnect();
  });
});