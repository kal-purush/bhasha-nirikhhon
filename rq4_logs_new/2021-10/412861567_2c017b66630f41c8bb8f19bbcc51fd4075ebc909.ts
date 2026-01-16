import { LitElement, html, css } from 'lit';
import { property } from 'lit/decorators.js';

export class DiplomacyWebsite extends LitElement {
  @property({ type: String }) title = 'Diplomacy Website';

  @property({ type: String }) url = '';

  @property({ type: String }) name = '';

  @property({ type: String }) background = '';

  @property({ type: String }) image = '';

  @property({ type: String }) caption = '';

  static styles = css`
    :host {
      display: grid;
    }
    #website {
      background-repeat: no-repeat;
      /*background-position: center;*/
      background-size: cover;
    }
    .play-text {
      background-color: white;
    }
    .content-padding {
      padding-left: 1rem;
      padding-right: 1rem;
    }
    .bottom-border {
      border-bottom: red;
      border-bottom-width: 0.25rem;
      border-bottom-style: solid;
    }
    a:link {
      color: aqua;
      background-color: white;
      text-decoration: underline;
    }

    a:visited {
      color: red;
      background-color: white;
      text-decoration: underline;
    }
    a:hover {
      color: orange;
      background-color: transparent;
      text-decoration: underline;
    }

    a:active {
      color: red;
      background-color: transparent;
      text-decoration: underline;
    }
  `;

  render() {
    return html`
      <div
        id="website"
        class="content-padding bottom-border"
        style="background-image: url('${this.background}');"
      >
        <h3><a href="${this.url}">${this.name}</a></h3>
        <div class="play-text">
          <slot></slot>
        </div>
        ${this.image !== ''
          ? html`
              <figure>
                <img src="${this.image}" alt="${this.name}" width="50%" />
                <figcaption class="caption">
                  <h5>${this.caption}</h5>
                </figcaption>
              </figure>
            `
          : html``}
      </div>
    `;
  }
}