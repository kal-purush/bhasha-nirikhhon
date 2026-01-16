import { IAuthData } from "../models/interfaces/IAuthData"
import { mockService } from "../helpers/MockState/MockService"
import { setIsAuthorized, setUserData } from "./sessionService";
import { navigate } from "../helpers/history";

export const signUp = (data: IAuthData) => {
  mockService.signUp(data)
    .then((user) => {
      setIsAuthorized();
      setUserData(user);
      navigate('/');
    });
};

export const signIn = (data: IAuthData) => {
  mockService.signIn(data)
    .then((user) => {
      setIsAuthorized();
      setUserData(user);
      navigate('/');
    });
}