import { VuexModule, Module, Action } from "vuex-module-decorators";
import User from "@ctsy/api-sdk/dist/modules/User";

@Module({})
export default class UserRegister extends VuexModule {
  RegisterInfo: any;

  @Action({ rawError: true })
  async REGISTER(data: any) {
    let info = {
      Name: data.Name,
      Nick: data.NickName,
      Account: data.Account,
      PWD: data.PWD,
      Sex: -1,
      PUID: 1,
      Contacts: [],
    };
    await User.AuthApi.regist(
      data.Name,
      data.NickName,
      data.Account,
      data.PWD,
      -1,
      1,
      "",
      []
    );
  }
}