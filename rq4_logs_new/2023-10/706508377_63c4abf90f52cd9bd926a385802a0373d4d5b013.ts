import { ValidationRuleAnswer } from '../../types';
import { ValidationRule } from '../../validation-rule';

export class TextStrictEqualValidationRule extends ValidationRule<'validate', string> {
  requirement = 'Значение должно быть {{strictString}}';

  constructor(private strictString: string) {
    super();
  }

  validate(value: string): ValidationRuleAnswer {
    return this.strictString === value
      ? this.returnSuccess('RunNextRule')
      : this.returnFail('SaveErrorAndRunNextRule');
  }
}