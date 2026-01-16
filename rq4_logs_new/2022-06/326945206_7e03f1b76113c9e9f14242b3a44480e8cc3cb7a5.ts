import { AbstractControl, FormBuilder, FormGroup, Validators } from '@angular/forms';

export class RewindNodeComponentFormResources {

  public static rewindNodeValidationMessages = {
    'rewindHeight': {
      'required': 'The rewind height is required.'
    }
  };

  public static buildRewindNodeForm(fb: FormBuilder): FormGroup {
    return fb.group({
      // eslint-disable-next-line @typescript-eslint/unbound-method
      'rewindHeight': ['', Validators.required]
    });
  }
}