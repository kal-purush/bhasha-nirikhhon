/**
@license
*/

import '@material/mwc-icon-button-toggle';
import { customElement, html, LitElement } from 'lit-element';
import { SharedStyles } from './shared-styles';

export interface ClockToggleDetail {
  isStarted: boolean;
}

const TOGGLE_EVENT_NAME = 'clock-toggled';
export class ClockToggleEvent extends CustomEvent<ClockToggleDetail> {
  static eventName = TOGGLE_EVENT_NAME;

  constructor(detail: ClockToggleDetail) {
    super(ClockToggleEvent.eventName, {
      detail,
      bubbles: true,
      composed: true
    });
  }
}

declare global {
  interface HTMLElementEventMap {
    [TOGGLE_EVENT_NAME]: ClockToggleEvent;
  }
}

// This element is *not* connected to the Redux store.
@customElement('lineup-game-clock')
export class LineupGameClock extends LitElement {
  protected render() {
    return html`
      ${SharedStyles}
      <style>
        :host { display: inline-block; }
      </style>
      <span>
        <span id="gamePeriod">Period: 1</span>
        <mwc-icon-button-toggle id='clockToggle' onIcon="pause_circle_outline" offIcon="play_circle_outline" label="Start/stop the clock"
        @MDCIconButtonToggle:change="${this._toggleClock}"></mwc-icon-button-toggle>
        <span id="periodTimer"></span>&nbsp;[<span id="gameTimer"></span>]
      </div>`
  }

  private _toggleClock(e: CustomEvent) {
    this.dispatchEvent(new ClockToggleEvent({isStarted: e.detail.isOn}));
  }
}