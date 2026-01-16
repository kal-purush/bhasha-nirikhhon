import { HttpService } from "src/app/Services/http.service";
import { Observable } from "rxjs";
import { map } from "rxjs/operators";
import { Directive } from "@angular/core";
import {
  AsyncValidator,
  AsyncValidatorFn,
  AbstractControl,
  ValidationErrors,
} from "@angular/forms";

@Directive({
  selector: "[appEmployeeUsernameAsyncValidator]",
})
export class EmployeeUsernameAsyncValidatorDirective implements AsyncValidator {
  private myHttp: HttpService;
  constructor(myHttp: HttpService) {
    this.myHttp = myHttp;
  }
  validate(control: AbstractControl): Observable<ValidationErrors | null> {
    return this.myHttp.getOne(`employees/username/${control.value}`).pipe(
      map((emp) => {
        return !emp ? null : { duplicateUsername: true };
      })
    );
  }
}