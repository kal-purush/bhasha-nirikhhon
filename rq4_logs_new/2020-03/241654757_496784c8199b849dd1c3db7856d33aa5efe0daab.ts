import { Tag } from "./tag";

describe("Tag", () => {
  test("should not create a tag from undefined", () => {
    const tagInfo: any = undefined;
    expect(() => new Tag(tagInfo, tagInfo, tagInfo)).toThrow(
      "must provide a tag name"
    );
  });

  test("should not create tag without a tag name", () => {
    const tagInfo = {
      name: "",
      postid: 5,
      color: "#00000"
    };
    expect(() => new Tag(tagInfo.name, tagInfo.postid, tagInfo.color)).toThrow(
      "must provide a tag name"
    );
  });

  test("should not create tag without a postid", () => {
    const info: any = undefined;
    const tagInfo = {
      name: "business",
      color: "#00000"
    };
    expect(() => new Tag(tagInfo.name, info, tagInfo.color)).toThrow(
      "must provide postid"
    );
  });

  test("should not create tag when name passes the size limit", () => {
    const tagInfo = {
      name: "ehfhgytnslfoengeldngnjengjr4rngjf dffdnfdgn",
      postid: 5,
      color: "#00000"
    };
    expect(() => new Tag(tagInfo.name, tagInfo.postid, tagInfo.color)).toThrow(
      "tag too long, MAX(25)"
    );
  });

  test("should create without providing a color & id", () => {
    const tagInfo = {
      name: "food",
      postid: 5
    };

    const newTag = new Tag(tagInfo.name, tagInfo.postid);
    expect(newTag).toBeDefined();
    expect(newTag.getColor()).toBe("#787878");
  });

  test("should create while providing an id", () => {
    const tagInfo = {
      name: "food",
      postid: 5,
      id: 1
    };

    const newTag = new Tag(tagInfo.name, tagInfo.postid, undefined, tagInfo.id);
    expect(newTag).toBeDefined();
    expect(newTag.getColor()).toBe("#787878");
    expect(newTag.getId()).toEqual(1);
    expect(newTag.getName()).toBe("food");
    expect(newTag.getPostId()).toEqual(5);
  });
});