import {
  Component,
  View,
  CORE_DIRECTIVES,
  FORM_DIRECTIVES,
  FormBuilder,
  ControlGroup,
  Control,
  Validators,
  Inject
} from 'angular2/angular2'

@Component({
  selector: 'model-driven-form'
})
@View({
  templateUrl: 'app/components/formsExp/modelDrivenForm/modelDrivenForm-component.html',
  directives: [FORM_DIRECTIVES, CORE_DIRECTIVES]
})
export class ModelDrivenFormComponent {

  form: ControlGroup

  // individually instantiated control
  name: Control = new Control('', Validators.required)

  constructor (@Inject(FormBuilder) formBuilder: FormBuilder) {
    this.form = formBuilder.group({
      'name': this.name,
      // simplified instatiation of control
      'password': ['', Validators.compose([Validators.required, customValidator])]
    })

    function customValidator (control) {
      // text should begin with 123
      if (!control.value.match(/^123/)){
        return {invalidSku: true};
      }
    }
  }

  onInit () {
    // Observe all changes to form, reminds me of $watch
    this.form.valueChanges.observer({
      next: (value) => {
        console.log("form changed to: ", value);
      }
    })
  }

  logForm () {
    console.log(this.form)
    console.log(this.form.errors)
  }

  onSubmit () {
    console.log(this.form)
  }
}