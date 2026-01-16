import { ValidationRuleAnswer } from '../../types';
import { ValidationRule } from '../../validation-rule';

export class OnlyLitinicOrCyrillicCharsValidationRule extends ValidationRule <'validate', string> {
  requirement = 'В строке может быть только латиница и кирилица';

  private languages = /^[ а-яёa-z0-9]+$/i;

  validate(value: string): ValidationRuleAnswer {
    return this.languages.test(value)
      ? this.returnSuccess('RunNextRule')
      : this.returnFail('SaveErrorAndRunNextRule');
  }
}