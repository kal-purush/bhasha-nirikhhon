import { AbstractControl, ValidationErrors } from "@angular/forms";
import { HttpService } from "../../../Services/http.service";

export class AsyncEmployeeValidator {
  constructor(private myHttp: HttpService) {}
  static async uniqueUsername(
    username: AbstractControl
  ): Promise<ValidationErrors | null> {
    let controlBind = this as any;
    let myHttp = controlBind.myHttp as HttpService;
    return new Promise((resolve) => {
      myHttp
        .getById("employees/check/user-name", username.value)
        .subscribe((result) => {
          if (result) {
            resolve({ takenUsername: true });
          }
          resolve(null);
        });
    });
  }
  static async uniqueEmail(
    email: AbstractControl
  ): Promise<ValidationErrors | null> {
    let controlBind = this as any;
    let myHttp = controlBind.myHttp as HttpService;
    return new Promise((resolve) => {
      myHttp
        .getById("employees/check/email", email.value)
        .subscribe((result) => {
          if (result) {
            resolve({ takenEmail: true });
          }
          resolve(null);
        });
    });
  }
}