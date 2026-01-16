import { Component } from "@angular/core";
import {
    AbstractControl,
    FormBuilder,
    FormGroup,
    Validators
} from "@angular/forms";
import { matchingPasswords } from "../../validators/matchingPasswords";
import { Router } from "@angular/router";
import { AuthApiService } from "../../services/api/auth.api.service";
import { RegisterRequestDTO } from "../../../../common/dto/register-request.dto";

@Component({
    selector: "registration-form",
    templateUrl: "./registration.component.html",
    styleUrls: ["./registration.component.less"]
})
export class RegistrationComponent {
    constructor(
        private fb: FormBuilder,
        private router: Router,
        private authService: AuthApiService
    ) {}

    serverErrors: number;

    registrationForm: FormGroup = this.fb.group(
        {
            email: [
                "",
                Validators.compose([
                    Validators.required,
                    Validators.pattern(
                        "^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,4}$"
                    )
                ])
            ],
            nickname: [
                "",
                Validators.compose([
                    Validators.required,
                    Validators.minLength(3),
                    Validators.maxLength(20)
                ])
            ],
            password: [
                "",
                Validators.compose([
                    Validators.required,
                    Validators.minLength(5),
                    Validators.maxLength(20)
                ])
            ],
            confirmPassword: ["", [Validators.required]],
            acceptTerms: [false, Validators.requiredTrue]
        },
        { validator: matchingPasswords("password", "confirmPassword") }
    );

    get _email(): AbstractControl {
        return this.registrationForm.get("email");
    }

    get _nickname(): AbstractControl {
        return this.registrationForm.get("nickname");
    }

    get _password(): AbstractControl {
        return this.registrationForm.get("password");
    }

    get _confirmPassword(): AbstractControl {
        return this.registrationForm.get("confirmPassword");
    }

    get acceptTerms(): AbstractControl {
        return this.registrationForm.get("acceptTerms");
    }

    submitHandler(): void {
        this.serverErrors = null;
        const newUser: RegisterRequestDTO = {
            email: this.registrationForm.value.email,
            nickname: this.registrationForm.value.nickname,
            password: this.registrationForm.value.password
        };
        if (this.registrationForm.valid) {
            this.authService.register(newUser).subscribe(
                () => {
                    this.router.navigate(["/login"]);
                },
                err => {
                    this.serverErrors = err.error.statusCode;
                }
            );
        }
    }
}