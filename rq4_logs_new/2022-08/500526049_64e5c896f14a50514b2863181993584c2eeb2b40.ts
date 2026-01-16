import { Config } from '@oclif/core';
import path from 'node:path';

import BaseCommand from '../../base-command';
import { getUserConfigExists, updateUserConfig } from '../../utilities/config.utilities';

/**
 * Handles local user configuration
 *
 * @see https://oclif.io/docs/config
 */
export default class InitUserConfig extends BaseCommand {
  static description = 'Update local .condohub config';

  static examples = [`$ condohub config update`];

  constructor(argv: string[], config: Config) {
    super(argv, config, { name: 'config:update' });
  }

  async run() {
    const { args } = await this.parse(InitUserConfig);
    const configPath = path.join(this.config.configDir, 'config.json');
    const userConfigExist = await getUserConfigExists(this.config);

    if (!userConfigExist) {
      this.logCommand(`no config.json found in ${configPath}, did you run 'condohub config init'?`);
      return;
    }
    this.logCommand(`config.json found in ${configPath}`);
    const userConfig = await updateUserConfig(this.config);
    this.logCommand(`config.json updated successfully`);
  }
}