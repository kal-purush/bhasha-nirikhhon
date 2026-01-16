import { LitElement, css, html, unsafeCSS } from 'lit'
import { customElement, property } from 'lit/decorators.js'

import { WithTheme } from '~/mixins'
import { defaultMSTheme } from '~/theme'

@customElement('ms-avatar-template')
export class MSAvatarTemplate extends WithTheme(LitElement) {
  static styles = css`
    :host {
      display: inline-block;
      width: 52px;
      height: 52px;
      border-radius: 50%;
      overflow: hidden;
      background-color: var(--primary-color, ${unsafeCSS(defaultMSTheme.colors.primary)});
      padding: 3px;
      box-sizing: border-box;
    }
    img {
      width: 100%;
      height: 100%;
      border-radius: 50%;
      object-fit: cover;
    }
  `

  @property({ type: String })
  src?: string

  @property({ type: String })
  atl?: string

  render() {
    return this.renderWithStyles(html`<img src=${this.src} atl=${this.atl} />`)
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'ms-avatar-template': MSAvatarTemplate
  }
}