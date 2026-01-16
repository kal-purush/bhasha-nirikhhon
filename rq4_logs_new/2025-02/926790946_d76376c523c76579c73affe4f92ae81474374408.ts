import { UserValidationsError } from "@/lib/CustomErrors";

export default class ResetPassword {
  constructor(
    readonly password: string,
    readonly passwordConfirmation: string,
  ) {}

  getNewPassword() {
    let newPassword = "";

    if (this.passwordValidation()) {
      newPassword = this.password;
    }

    return newPassword;
  }

  private passwordValidation() {
    const passwordRegex = /^(?=.*[A-Z])(?=.*\d)(?=.*[!@#$%^&*()_+\-=[\]{};':"\\|,.<>\/?]).{8,}$/;

    if (!passwordRegex.test(this.password)) {
      throw new UserValidationsError(
        "Formato de senha inválida.",
        "A senha deve conter: 8 Caracteres, 1 Letra maiúscula, 1 Número e 1 Caractere especial.",
        400,
        true,
        "A senha não segue o seguinte formato: 8 Caracteres, 1 Letra maiúscula, 1 Número e 1 Caractere especial.",
      );
    }

    if (this.password !== this.passwordConfirmation) {
      throw new UserValidationsError(
        "As senhas não são iguais.",
        "Verifique as senhas informadas.",
        400,
        true,
        "As senhas não são iguais.",
      );
    }

    return true;
  }
}