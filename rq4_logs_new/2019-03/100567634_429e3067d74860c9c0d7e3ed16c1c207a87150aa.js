/**
@license
Copyright 2016 The Advanced REST client authors <arc@mulesoft.com>
Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
*/
import { PolymerElement } from '../../@polymer/polymer/polymer-element.js';

import '../../@polymer/paper-item/paper-item.js';
import '../../@polymer/paper-dropdown-menu/paper-dropdown-menu.js';
import '../../@polymer/paper-listbox/paper-listbox.js';
import '../../variables-consumer-mixin/variables-consumer-mixin.js';
import { html } from '../../@polymer/polymer/lib/utils/html-tag.js';
/**
 * An element to select current variables environment.
 *
 * Renders a material design dropdown with list of available environments.
 * It always render environment **Default** (value `default`).
 *
 * ### Example
 *
 * ```html
 * <environment-selector></environment-selector>
 * ```
 *
 * ```javascript
 * document.queryElement('environment-selector')
 * .addEventListener('selected-environment-changed', (e) => {
 *    console.log(e.detail.value); // Selected environment
 * });
 * ```
 *
 * ### Styling
 *
 * `<environment-selector>` provides the following custom properties and mixins for styling:
 *
 * Custom property | Description | Default
 * ----------------|-------------|----------
 * `--environment-selector` | Mixin applied to the element | `{}`
 *
 * Use variables for `paper-dropdown-menu`, `paper-listbox` and `paper-item`
 * to style the control.
 *
 * @polymer
 * @customElement
 * @memberof UiElements
 * @demo demo/index.html
 * @appliesMixin ArcComponents.VariablesConsumerMixin
 */
class EnvironmentSelector extends ArcComponents.VariablesConsumerMixin(PolymerElement) {
  static get template() {
    return html`
    <style>
    :host {
      display: block;
      @apply --environment-selector;
    }
    </style>
    <paper-dropdown-menu label="Environment" no-label-float="[[noLabelFloat]]" dynamic-align="">
      <paper-listbox slot="dropdown-content" selected="{{selected}}" attr-for-selected="value">
        <paper-item value="default">Default</paper-item>
        <template is="dom-repeat" items="[[environments]]" id="envRepeater">
          <paper-item value\$="[[item.name]]">[[item.name]]</paper-item>
        </template>
      </paper-listbox>
    </paper-dropdown-menu>
`;
  }

  static get is() {return 'environment-selector';}
  static get properties() {
    return {
      /**
       * Set to make selector's label dissapear after selection has been made.
       */
      noLabelFloat: Boolean,
      /**
       * Selected environment.
       */
      selected: {
        type: String,
        notify: true,
        observer: '_environmentChanged'
      }
    };
  }

  constructor() {
    super();
    this._envChangedHandler = this._envChangedHandler.bind(this);
  }

  connectedCallback() {
    super.connectedCallback();
    window.addEventListener('selected-environment-changed', this._envChangedHandler);
  }

  disconnectedCallback() {
    super.disconnectedCallback();
    window.removeEventListener('selected-environment-changed', this._envChangedHandler);
  }

  /**
   * Handler for the `selected-environment-changed` event.
   * @param {CustomEvent} e
   */
  _envChangedHandler(e) {
    if (e.cancelable || e.composedPath()[0] === this) {
      return;
    }
    this.__cancelEnvChange = true;
    this.set('selected', e.detail.value);
    this.__cancelEnvChange = false;
  }
  /**
   * Handler for the `selected` property change
   * @param {String} selected
   */
  _environmentChanged(selected) {
    if (this.__cancelEnvChange) {
      // the change has been caused by the external source
      return;
    }
    if (selected === null) {
      this.set('selected', undefined);
      return;
    }
    if (selected === undefined) {
      return;
    }
    this._dispatchChange(selected);
  }
  /**
   * Dispatches `selected-environment-changed` custom event
   * @param {String} value New value
   * @return {CustomEvent} Dispatched event
   */
  _dispatchChange(value) {
    const e = new CustomEvent('selected-environment-changed', {
      bubbles: true,
      composed: true,
      detail: {
        value
      }
    });
    this.dispatchEvent(e);
    return e;
  }
  /**
   * Fired when selected environment changed.
   *
   * @event selected-environment-changed
   * @param {String} value Name of selected environment.
   */
}
window.customElements.define(EnvironmentSelector.is, EnvironmentSelector);