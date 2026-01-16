import {
  Component,
  FORM_DIRECTIVES,
  FormBuilder,
  ControlGroup,
  Control
} from 'angular2/angular2'

@Component({
  selector: 'form-observable',
  templateUrl: 'app/components/formObservable/formObservable-component.html',
  directives: [FORM_DIRECTIVES]
})
export class FormObservableComponent {

  form: ControlGroup

  name: Control = new Control('')

  constructor (formBuilder: FormBuilder) {
    this.form = formBuilder.group({
      'name': this.name
    })
  }

  onInit () {
    // Observe all changes to form, reminds me of $watch
    this.form.valueChanges.observer({
      next: (value) => {
        console.log("form changed to: ", value);
      }
    })
  }

  onSubmit () {
    alert('wat')
  }
}