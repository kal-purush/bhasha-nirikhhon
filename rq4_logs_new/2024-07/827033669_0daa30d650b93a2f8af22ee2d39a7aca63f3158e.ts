import { LitElement, css, html } from 'lit';
import { customElement, property, query } from 'lit/decorators.js';
import todoStore, { ITodoStore } from '../../store/todo';

@customElement('todo-input')
export class TodoInput extends LitElement {
  static styles = css`
    :host {
      display: flex;
      width: 96%;
      height: 48px;
      position: relative;
      margin: 1rem auto 2rem auto;
      border-radius: 1rem;
    }

    input {
      font-size: 1rem;
      color: rgb(var(--kemet-color-gray-300));
      width: 100%;
      padding: 0 1.5rem;
      border: 0;
      outline: 6px solid rgb(var(--kemet-color-black) / 25%);
      border-radius: 9999px;
      background: rgb(var(--kemet-color-black) / 25%);
    }

    button {
      cursor: pointer;
      font-size: 1.25rem;
      position: absolute;
      right: 0;
      padding: 0 12%;
      height: 48px;
      border: 0;
      border-radius: 9999px;
    }
  `;

  @property()
  todoState: ITodoStore = todoStore.getInitialState();

  @property()
  label: string = 'Add';

  @query('input')
  todoInput!: HTMLInputElement;

  render() {
    return html`
      <input />
      <button @click=${() => this.handleClick()}>${this.label}</button>
    `
  }

  handleClick() {
    this.todoState.addTodo({ value: this.todoInput.value, checked: false });
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'todo-input': TodoInput
  }
}