/**
@license
*/

import { PageObject, PageOptions, PageOpenFunction } from './page-object';

export class AddTeamDialog extends PageObject {

  constructor(options: PageOptions = {}) {
    super({
      ...options,
      scenarioName: 'addNewTeam',
    });
  }

  protected get openFunc(): PageOpenFunction | undefined {
    return async () => {
      await this.selectTeam('addnewteam');
    }
  }

  async selectTeam(teamId: string) {
    await this.page.evaluate((teamId: string) => {
      const app = document.querySelector('lineup-app');
      const selector = app!.shadowRoot!.querySelector('lineup-team-selector');
      const list = selector!.shadowRoot!.querySelector('paper-dropdown-menu paper-listbox');
      (list as any).select(teamId);
    }, teamId);
  }
}