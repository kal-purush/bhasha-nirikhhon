import { BaseComponent, Component } from "./../../component.js";

export interface LoginContainer extends Component {
  userId: string;
  password: string;
}
export class LoginSectionInput extends BaseComponent<HTMLElement> implements LoginContainer {
  constructor() {
    super(`<div>
            <h3>로그인</h3>
            <div class="form__container">
                <label for="userId">아이디</label>
                <input type="text" id="userId" />
            </div>
            <div class="form__container">
                <label for="password">비밀번호</label>
                <input type="text" id="password" />
            </div>
        </div>`);
  }

  public get userId(): string {
    const element = this.element.querySelector("#userId")! as HTMLInputElement;
    return element.value;
  }
  public get password(): string {
    const element = this.element.querySelector("#password")! as HTMLInputElement;
    return element.value;
  }
}