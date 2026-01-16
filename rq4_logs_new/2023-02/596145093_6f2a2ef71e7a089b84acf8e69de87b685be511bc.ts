import { User } from "../../domain/models/user";
import { ReadUsers } from "../../domain/useCases/read-users";
import { ServerError } from "../errors/server-error";
import { ReadUsersController } from "./read-users";

interface ControllerTypes {
  sut: ReadUsersController
  readUsers: ReadUsers
}

const makeReadUsers = (): ReadUsers => {
  class ReadUsersStub implements ReadUsers {
    async read(): Promise<User[]> {
      const fakeUsers: User[] = [
        {
          "id": 1,
          "name": "explicabo alias hic reprehenderit deleniti quos id reprehenderit consequuntur ipsam iure voluptatem ea culpa excepturi ducimus repudiandae ab",
          "price": 6945
        },
        {
          "id": 2,
          "name": "nostrum veritatis reprehenderit repellendus vel numquam soluta ex inventore ex",
          "price": 2435
        },
      ];

      return new Promise(resolve => resolve(fakeUsers));
    }
  }

  return new ReadUsersStub();
}

const makeController = (): ControllerTypes => {
  const readUsers = makeReadUsers();
  const sut = new ReadUsersController(readUsers);

  return {
    sut,
    readUsers
  }
}

describe("", () => {
  it('Should return 500 if ReadUsers throws', async () => {
    // given
    const { sut, readUsers } = makeController();
    
    // when
    jest.spyOn(readUsers, "read").mockImplementationOnce(() => {
      throw new Error();
    })
    const httpResponse = await sut.handle({ body: "" });

    // then
    expect(httpResponse.statusCode).toBe(500);
    expect(httpResponse.body).toEqual(new ServerError());
  })
})