import Urls from "../../../LBPro/TEZPower/FrontendSideApp/power-portal/src/constants/urls";
import axios from "../../../LBPro/TEZPower/FrontendSideApp/power-portal/src/utils/axios";
import { LoginAccountResult } from "../../../LBPro/TEZPower/FrontendSideApp/power-portal/src/typedef/account";
import { AxiosResponse } from "axios";
import { ApiResult } from "../../../LBPro/TEZPower/FrontendSideApp/power-portal/src/typedef/apiResults";
import { StorageUserName, StorageTokensName } from "../../../LBPro/TEZPower/FrontendSideApp/power-portal/src/constants/constants";

const login = async (
  username: string,
  password: string
): Promise<ApiResult<LoginAccountResult>> => {
  let user = {
    login: username.trim(),
    password: password
  };

  let response: AxiosResponse<ApiResult<LoginAccountResult>> = await axios.post<
    ApiResult<LoginAccountResult>
  >(Urls.User.Login, user);
  return response.data;
};

const logout = (): void => {
  localStorage.removeItem(StorageUserName);
  localStorage.removeItem(StorageTokensName);
};

export const accountService = {
  login,
  logout
};