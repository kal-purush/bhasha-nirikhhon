import Block from '../../utils/Block';
import template from './Chats.hbs';
import MessageForm from './containers/MessageForm';
import CreateChatModal from './containers/CreateChatModal';
import FormField from '../../components/FormField';
import Input from '../../components/Input';
import validateField from '../../utils/validateField';
import submitForm from '../../utils/submitForm';
import Button from '../../components/Button';
import router from '../../utils/Router';
import chatsController from './controllers/ChatsController';
import { CreateChatData } from './api/ChatsAPI';
import ChatInfos from './containers/ChatInfos';
import { withCurrentTitle } from '../../utils/connect';
import { withChats, withMessages } from './utils/chatConnects';
import ChatHistory from './containers/ChatHistory';
import { UserData } from './api/UserAPI';
import store from '../../utils/Store';
import ChatMessages from './containers/ChatMessages';

const ChatInfosWithChats = withChats(ChatInfos as typeof Block);
const ChatMessagesWithMessages = withMessages(ChatMessages as typeof Block);
const ChatHistoryWithCurrentTitle = withCurrentTitle(ChatHistory as typeof Block);

const deleteButtonLabelHTML = `
<div class="chat-history__header-menu-button-wrapper">
<svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
    <line x1="4.11077" y1="4.11103" x2="11.8889" y2="11.8892" stroke="#3369F3" stroke-width="1.5"/>
    <line x1="4.11078" y1="11.8891" x2="11.889" y2="4.11093" stroke="#3369F3" stroke-width="1.5"/>
</svg>
</div>

<p class="chat-history__header-menu-button-text">Удалить пользователя</p>
`;

const addButtonLabelHTML = `
<div class="chat-history__header-menu-button-wrapper">
    <svg width="12" height="12" viewBox="0 0 12 12" fill="none" xmlns="http://www.w3.org/2000/svg">
        <line x1="5.99988" y1="0.5" x2="5.99988" y2="11.5" stroke="#3369F3" stroke-width="1.5"/>
        <line x1="0.499878" y1="6" x2="11.4999" y2="6" stroke="#3369F3" stroke-width="1.5"/>
    </svg>
</div>

<p class="chat-history__header-menu-button-text">Добавить пользователя</p>
`;

export default class ChatsPage extends Block {
  constructor() {
    super();
    chatsController.getChats();
    (this.children.createChatModal as Block).hide();
    (this.children.addUserModal as Block).hide();
    (this.children.deleteUserModal as Block).hide();
  }

  initChildren() {
    this.children.chatInfos = new ChatInfosWithChats({
      chats: [],
    });
    this.children.chatHistory = new ChatHistoryWithCurrentTitle({
      chatMessages: new ChatMessagesWithMessages({
        messages: [],
      }),
      buttonDeleteUser: new Button({
        label: deleteButtonLabelHTML,
        type: 'button',
        class: 'chat-history__header-menu-button',
        events: {
          click: () => (this.children.deleteUserModal as Block).show(true),
        },
      }),
      buttonAddUser: new Button({
        label: addButtonLabelHTML,
        type: 'button',
        class: 'chat-history__header-menu-button',
        events: {
          click: () => (this.children.addUserModal as Block).show(true),
        },
      }),
      messageForm: new MessageForm({
        input: new Input({
          label: 'Сообщение',
          type: 'text',
          name: 'message',
          class: 'message',
          regex: '.+',
          events: {
            focus: validateField('Сообщение не может быть пустым'),
            blur: validateField('Сообщение не может быть пустым'),
          },
        }),
        events: {
          submit: (evt: Event) => submitForm<{message: string}>(evt, (data) => {
            const { socket } = store.getState();
            if (socket) {
              socket.send(JSON.stringify({
                content: data.message,
                type: 'message',
              }));
            }
          }),
        },
      }),
    });
    this.children.addUserModal = new CreateChatModal({
      formField: new FormField({
        label: 'Логин',
        name: 'login',
        class: 'auth-form__field',
        input: new Input({
          label: 'Логин',
          type: 'text',
          name: 'login',
          class: 'auth-form__field',
        }),
      }),
      button: new Button({
        label: 'Добавить пользователя',
        type: 'submit',
        class: 'button_with-margin',
      }),
      events: {
        submit: (evt: Event) => submitForm<UserData>(evt, (data) => {
          chatsController.addUserToChat(data, () => {
            (this.children.addUserModal as Block).hide();
          });
        }),
      },
    });
    this.children.deleteUserModal = new CreateChatModal({
      formField: new FormField({
        label: 'Логин',
        name: 'login',
        class: 'auth-form__field',
        input: new Input({
          label: 'Логин',
          type: 'text',
          name: 'login',
          class: 'auth-form__field',
        }),
      }),
      button: new Button({
        label: 'Удалить пользователя',
        type: 'submit',
        class: 'button_with-margin',
      }),
      events: {
        submit: (evt: Event) => submitForm<UserData>(evt, (data) => {
          chatsController.deleteUserToChat(data, () => {
            (this.children.deleteUserModal as Block).hide();
          });
        }),
      },
    });
    this.children.createChatModal = new CreateChatModal({
      formField: new FormField({
        label: 'Название чата',
        name: 'title',
        class: 'auth-form__field',
        input: new Input({
          label: 'Название чата',
          type: 'text',
          name: 'title',
          class: 'auth-form__field',
        }),
      }),
      button: new Button({
        label: 'Создать чат',
        type: 'submit',
        class: 'button_with-margin',
      }),
      events: {
        submit: (evt: Event) => submitForm<CreateChatData>(evt, (data) => {
          chatsController.createChat(data, () => {
            (this.children.createChatModal as Block).hide();
            chatsController.getChats();
          });
        }),
      },
    });

    this.children.linkProfile = new Button({
      label: 'Профиль >',
      type: 'button',
      class: 'link',
      events: {
        click: () => router.go('/settings'),
      },
    });
    this.children.buttonOpenCreateChatModal = new Button({
      label: 'Добавить чат',
      type: 'button',
      class: 'button_with-margin',
      events: {
        click: () => (this.children.createChatModal as Block).show(true),
      },
    });
  }

  render() {
    return this.compile(template, {});
  }
}