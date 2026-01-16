import Block from '../../utils/Block';
import { Button } from '../Button';
import template from './item.hbs';
import ChatsController from '../../controllers/ChatsController';

interface ItemProps {
  name: string;
  avatar: string;
  id: number;
  selectedChat: number | undefined;
}

export class ChatUsers extends Block {
  constructor(props: ItemProps) {
    super({
      ...props
    });
  }

  init() {
    this.children.delete = new Button({
      label: 'Удалить',
      class: 'profile__info-title profile__info-title_red button_st button_end',
      type: 'button',
      onClick: () => ChatsController.deleteUser(this.props.selectedChat as number, this.props.id),
    });
  }

  render() {
    return this.compile(template, this.props);
  }
}