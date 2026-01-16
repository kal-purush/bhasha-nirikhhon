import { View } from './View';
import { User, UserProps } from '../models/User';
import { UserForm } from "./UserForm";
import { UserShow } from "./UserShow";

export class UserEdit extends View<UserProps, User> {
  regionsMap(): { [key: string]: string } {
    return {
      userShow: '.user-show',
      userForm: '.user-form',
    };
  }

  onRender(): void {
    new UserShow(this.regions.userShow, this.modal).render();
    new UserForm(this.regions.userForm, this.modal).render();
  }

  template(): string {
    return `
      <div>
        <div class="user-show"></div>
        <div class="user-form"></div>
      </div>
    `;
  }
}