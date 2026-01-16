import * as core from "@actions/core";

import { Meta } from "./core";
import { ActionMeta, Input } from "./decorators";
import { objectEntries } from "./utils";

const fixture = {
  id: "123",
  name: "John Doe",
  age: 42,
  height: 192.1,
  isMember: false,
  isPremium: true,
  date: new Date("2023-01-01T09:00:00"),
};

@ActionMeta({ name: "Test Action", description: "Test Action Description" })
class TestAction extends Meta<TestAction> {
  @Input({ description: "id" })
  id: string;

  @Input({ description: "name", parser: "string" })
  name: string;

  @Input({ description: "age", parser: "number" })
  age: number;

  @Input({ description: "height", parser: "number" })
  height: number;

  @Input({ description: "isMember", parser: "boolean" })
  isMember: boolean;

  @Input({ description: "isPremium", parser: "boolean" })
  isPremium: boolean;

  @Input({ description: "date", parser: (value) => new Date(value) })
  date: Date;

  constructor() {
    super();

    this.id = this.getInput("id");
    this.name = this.getInput("name");
    this.age = this.getInput("age");
    this.height = this.getInput("height");
    this.isMember = this.getInput("isMember");
    this.isPremium = this.getInput("isPremium");
    this.date = this.getInput("date");
  }
}

describe("core", () => {
  const spy = jest.spyOn(core, "getInput").mockImplementation((name) => {
    const target = objectEntries(fixture).find(([key]) => key === name)?.[1];

    return target instanceof Date
      ? target.toISOString()
      : target?.toString() ?? "";
  });

  afterAll(() => {
    spy.mockRestore();
  });

  describe("指定したキーの値を対応する型にパースした値を返す", () => {
    const sut = new TestAction();

    it.each(objectEntries(fixture))("%s: %s", (key, value) => {
      expect(sut[key]).toStrictEqual(value);
    });
  });
});