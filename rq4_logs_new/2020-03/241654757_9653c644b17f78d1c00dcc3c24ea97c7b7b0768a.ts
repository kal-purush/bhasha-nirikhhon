import DConnection from "../../database";
import Tagdb from "../../data-access/tag.db";
import TagUseCase from "./tagUseCase";

let dConn: DConnection;
let tDB: Tagdb;
let tUseCase: TagUseCase;

beforeAll(() => {
  dConn = new DConnection();
  tDB = new Tagdb(dConn);
  tUseCase = new TagUseCase(tDB);
});

afterAll(async () => {
  await dConn.Close();
});

describe("Tag use case", () => {
  test("should bring all 0 tags", async () => {
    const tags = await tUseCase.BringAllTags();
    expect(tags).toHaveLength(0);
  });

  test("should create 3 tags to post 5", async () => {
    const listo = [{ name: "food" }, { name: "sushi" }, { name: "japan" }];

    const res = await tUseCase.AddTags(listo, 5, 5);

    expect(res).toBe("tags created");
  });

  test("should bring my 3 tags of post 5", async () => {
    const res = await tUseCase.BringMyTags(5);

    expect(res).toHaveLength(3);
    expect(res[0].name).toBe("food");
  });

  test("should search for a tag named sushi and found it", async () => {
    const res = await tUseCase.BringTag("sushi");

    expect(res.name).toBe("sushi");
    expect(res.id).toBeDefined();
  });

  test("should delete all 3 tags from post 5", async () => {
    const listo = [{ name: "food" }, { name: "sushi" }, { name: "japan" }];

    const res = await tUseCase.RemoveTagsTest(listo);

    expect(res).toBe("tags deleted");
  });

  test("should throw error when not finding a post at removing tags", async () => {
    const listo = [{ id: 8 }, { id: 9 }, { id: 10 }];

    await expect(tUseCase.RemoveTags(listo, 5, 1)).rejects.toThrowError(
      "post does not exist"
    );
  });

  test("should throw error when not finding the correct user at removing tags", async () => {
    const listo = [{ id: 8 }, { id: 9 }, { id: 10 }];

    await expect(tUseCase.RemoveTags(listo, 7, 5)).rejects.toThrowError(
      "cant remove tags to this post"
    );
  });
});