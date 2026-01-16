import { LitElement, html, css } from 'lit';
import { customElement } from 'lit/decorators.js';

@customElement('inspect-app')
class InspectApp extends LitElement {
  static styles = [
    css`
      :host {
        position: relative;

        display: flex;
        flex: 1;
        flex-direction: column;
      }
    `,
  ];

  render() {
    return html` <div class=""></div> `;
  }
}