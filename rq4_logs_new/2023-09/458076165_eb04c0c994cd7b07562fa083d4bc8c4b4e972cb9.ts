import { FormArray, FormControl, FormGroup } from '@angular/forms';
import {
  QuestionFormGroupModel,
  TextQuestionFormModel,
} from '../models/text-question-form.model';

export class GoogleFormCloneHelper {
  static newTextFormGroup() {
    const typedForm: FormGroup<TextQuestionFormModel> =
      new FormGroup<TextQuestionFormModel>({
        id: new FormControl('q1'),
        orderNo: new FormControl(2),
        type: new FormControl('question'),
        required: new FormControl(false),
        question: new FormGroup<QuestionFormGroupModel>({
          text: new FormControl(''),
          placeholder: new FormControl(''),
          type: new FormControl('freeText'),
          selectedAnswer: new FormControl(''),
          offeredAnswers: new FormArray([]),
        }),
      });

    return typedForm;
  }

  static get newSingleSelection() {
    const typedForm: FormGroup<TextQuestionFormModel> =
      new FormGroup<TextQuestionFormModel>({
        id: new FormControl('q1'),
        orderNo: new FormControl(2),
        type: new FormControl('question'),
        required: new FormControl(false),
        question: new FormGroup<QuestionFormGroupModel>({
          text: new FormControl(''),
          placeholder: new FormControl(''),
          type: new FormControl('freeText'),
          selectedAnswer: new FormControl(''),
          offeredAnswers: new FormArray([
            // first option
            new FormGroup({
              id: new FormControl('01'),
              orderNo: new FormControl(1),
              value: new FormControl('Yes'),
              remarkAnswer: new FormControl(true),
              remarkAnswerValue: new FormControl('idk'),
            }),
            //second option
            new FormGroup({
              id: new FormControl('01'),
              orderNo: new FormControl(1),
              value: new FormControl('Yes'),
              remarkAnswer: new FormControl(true),
              remarkAnswerValue: new FormControl('idk'),
            }),
          ]),
        }),
      });

    return typedForm;
  }
}