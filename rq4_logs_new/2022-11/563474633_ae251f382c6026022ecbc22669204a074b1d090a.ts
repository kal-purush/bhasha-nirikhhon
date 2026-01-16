import { UserState } from "./types";
import { loginUserActionCreator, userReducer } from "./userSlice";

describe("Given a userReducer", () => {
  const notLoggedUser: UserState = {
    id: "",
    isLogged: false,
    token: "",
    userName: "",
    email: "",
  };
  describe("When it receives the initial state and an empty action", () => {
    test("Then it should return a copy of the intial state", () => {
      const unknownAction = {
        type: "user/unknownAction",
      };

      const newState = userReducer(notLoggedUser, unknownAction);

      expect(newState).toStrictEqual(notLoggedUser);
    });
  });

  describe("When it receives the initial state and a login user action with a new user", () => {
    test("Then it should return the state with the users credentials and isLogged true", () => {
      const newUser = {
        userName: "Paco",
        password: "Paco123",
        email: "Paco@gmail.com",
        token: "",
        id: "",
      };

      const expectedState: UserState = {
        ...newUser,
        isLogged: true,
      };

      const newState = userReducer(
        notLoggedUser,
        loginUserActionCreator(newUser)
      );

      expect(newState).toStrictEqual(expectedState);
    });
  });
});