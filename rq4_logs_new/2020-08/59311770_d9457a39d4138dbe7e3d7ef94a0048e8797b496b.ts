/**
@license
*/

import { customElement, html } from 'lit-element';
import { connect } from 'pwa-helpers/connect-mixin.js';
import { addNewTeam } from '../actions/team';
import team from '../reducers/team';
import { RootState, store } from '../store';
import { EVENT_NEWTEAMCREATED } from './events';
import './lineup-team-create';
import { PageViewElement } from './page-view-element';
import { SharedStyles } from './shared-styles';

// We are lazy loading its reducer.
store.addReducers({
  team
});

// This element is connected to the Redux store.
@customElement('lineup-view-team-create')
export class LineupViewTeamCreate extends connect(store)(PageViewElement) {
  protected render() {
    return html`
      ${SharedStyles}
      <section>
        <h2>New Team</h2>
        <lineup-team-create></lineup-team-create>
      </section>
    `;
  }

  protected firstUpdated() {
    window.addEventListener(EVENT_NEWTEAMCREATED, this._newTeamCreated.bind(this) as EventListener);
  }

  stateChanged(state: RootState) {
    if (!state.team) {
      return;
    }
  }

  private _newTeamCreated(e: CustomEvent) {
    store.dispatch(addNewTeam(e.detail.team));
  }

}