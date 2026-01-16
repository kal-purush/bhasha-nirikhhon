import { 
  Component,
  View,
  Control,
  ControlGroup,
  ControlArray,
  Validators,
  CORE_DIRECTIVES,
  FORM_DIRECTIVES
} from 'angular2/angular2';

	 

@Component({
	selector: "forms-snippet"
})


@View({
	template: `
		<h2>Forms Snippet</h2>
		<p>
			Illustrates using forms
		</p>
		
		<h3>Issues</h3>
		<p>
			1 - Can't find a way of setting the data in the form once it's been loaded - having to resort to rebuilding the form!
		</p>
		
		<h3>Resources</h3>
		 - <a href="http://plnkr.co/edit/23DESOpbNnBpBHZt1BR4?p=preview">Plunkr illustration</a>
		
		<h3>Snippet</h3>
		<hr/>
				
		<div>
			<form [ng-form-model]="pwdForm">
				<div>
					<label for="email">email</label>
					<input id="email" ng-control="email">
					<span class="error-ind" *ng-if="!emailCtrl.valid">*</span>
				</div>
				<div ng-control-group="pwd">
					<div>
						<label for="password">password</label>
						<input id="password" ng-control="password">
						<span class="error-ind" *ng-if="!passwordCtrl.valid">*</span>
					</div>
					<div>
						<label for="pwd-conf">password</label>
						<input id="pwd-conf" ng-control="passwordConfirm">
						<span class="error-ind" *ng-if="!passwordConfirmCtrl.valid">*</span>
					</div>
				</div>
				
				<button (click)="emulateLoad()">Emulate data load</button>
			</form>
		</div>
		
		<div>
			<h3>Value:</h3>
			<pre>{{cgValue}}</pre>
			<h3>Validity:</h3>
			<pre>{{pwdForm.valid}}</pre>
		</div>
		
		<style scope>
			label {
				width: 150px;
				display: inline-block;
			}
			.error-ind {
				color: red;
				font-size: large;
			}
		</style>
	`,
	directives: [CORE_DIRECTIVES, FORM_DIRECTIVES]
})

export class FormsSnippet {
	pwdForm: ControlGroup = null;
	emailCtrl: Control = null;
	passwordCtrl: Control = null;
	passwordConfirmCtrl: Control = null;
	
	constructor() {
		this.setupForm();
	}
	
	setupForm(withData) {
		if (withData === undefined) {
			// use empty set
			withData = {
				email: "",
				password: "",
				passwordConfirm: ""
			}
		}
		
		this.emailCtrl = new Control(withData.email, Validators.required);
		this.passwordCtrl = new Control(withData.password, Validators.required);
		this.passwordConfirmCtrl = new Control(withData.passwordConfirm, Validators.required);
		
		this.pwdForm = new ControlGroup({
			email: this.emailCtrl,
			pwd: new ControlGroup({
				password: this.passwordCtrl,
				passwordConfirm: this.passwordConfirmCtrl
			})
		});		
	}
  
  get cgValue(): string {
    return JSON.stringify(this.pwdForm.value, null, 2);
  }	
	
	/// MUST be a better way of doing this!
	emulateLoad() {
		let data = {
			email: "bob@sky.com",
			password: "lots",
			passwordConfirm: "of letters and symbols"
		};
		
		setTimeout(() => {
			// emulate an Http request callback
			this.setupForm(data);
		}, 1000);		
	}
  
}