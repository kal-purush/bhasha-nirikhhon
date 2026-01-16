import { createOneDoc, findOneDoc } from "../crud__operation";
import { User } from "../models/User";

describe("insert", () => {
  it("should insert a doc into collection", async () => {
    const mockUser: User = new User(
      "7BNE8r{?3Xn5~xF7",
      "John",
      "%~eGJ9!!DWy=4KA4"
    );
    await createOneDoc(mockUser);

    const query: object = { username: "John" };
    const insertedUser = await findOneDoc(query);
    expect(insertedUser);
  });
  it("should not receive the document", async () => {
    const query: object = { username: "Jackson" };
    const insertedUser = await findOneDoc(query);
    expect(insertedUser);
  });
});