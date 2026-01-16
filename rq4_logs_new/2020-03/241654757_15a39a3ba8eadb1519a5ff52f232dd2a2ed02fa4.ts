import DConnection from "../database";
import Tagdb from "./tag.db";
import { Tag } from "../entities/tag";

let dConn: DConnection;
let tDB: Tagdb;

beforeAll(() => {
  dConn = new DConnection();
  tDB = new Tagdb(dConn);
});

afterAll(async () => {
  await dConn.Close();
});

describe("Tag DB", () => {
  test("should bring all tags", async () => {
    const tags = await tDB.getTags();
    expect(tags).toHaveLength(0);
  });

  test("should create Tag", async () => {
    const post = new Tag("jest", 5);
    const repost = await tDB.insertTag(post);
    expect(repost.name).toEqual("jest");
    expect(repost.id).toBeDefined();
  });

  test("should search for a tag", async () => {
    const repost = await tDB.searchTag("jest");
    expect(repost.name).toEqual("jest");
    expect(repost.id).toBeDefined();
  });

  test("should delete tag", async () => {
    const repost = await tDB.deleteTagTest("jest");
    expect(repost).toEqual("jest");
  });

  test("should create Tags", async () => {
    const post1 = new Tag("jest", 5);
    const post2 = new Tag("jest2", 5);
    const post3 = new Tag("jest3", 5);
    let tags: Tag[] = [];
    tags.push(post1);
    tags.push(post2);
    tags.push(post3);
    const repost = await tDB.insertTags(tags);
    expect(repost).toBe("tags created");
  });

  test("should bring my tags", async () => {
    const repost = await tDB.getMyTags(5);
    expect(repost).toHaveLength(3);
  });

  test("should delete all tags", async () => {
    const names = [{ name: "jest" }, { name: "jest2" }, { name: "jest3" }];
    const repost = await tDB.deleteTagsTest(names);
    expect(repost).toBe("tags deleted");
  });
});