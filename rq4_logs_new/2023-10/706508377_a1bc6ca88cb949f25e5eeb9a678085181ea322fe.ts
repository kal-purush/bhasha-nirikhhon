import { ValidationRuleAnswer } from '../../types';
import { ValidationRule } from '../../validation-rule';

export class NoContainedCharsValidationRule extends ValidationRule <'validate', string> {
  requirement = 'Строка не должна содержать символы {{noChars}}';

  constructor(private noChars: string, requirement?: string) {
    super();
    if (requirement) this.requirement = requirement;
  }

  validate(value: string): ValidationRuleAnswer {
    // eslint-disable-next-line no-restricted-syntax
    for (const char of this.noChars) {
      if (value.includes(char)) {
        return this.returnFail('SaveErrorAndRunNextRule', { noChars: this.noChars });
      }
    } return this.returnSuccess('SuccessRunNextRule');
  }
}