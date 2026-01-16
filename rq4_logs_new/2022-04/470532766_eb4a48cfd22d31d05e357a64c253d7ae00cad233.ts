import { compile } from 'pug';
import { Block } from '../../../utils/Block/index';
import { settingsButtonTemplate } from './SettingsButton.template';
import { SettingsButtonProps } from './SettingsButton.types';

export default class SettingsButton extends Block<SettingsButtonProps> {
  public constructor(props: SettingsButtonProps) {
    super(
      'div',
      {
        ...props,
      },
    );
  }

  public render() {
    return this.compile(compile(settingsButtonTemplate), { ...this.props });
  } 
}