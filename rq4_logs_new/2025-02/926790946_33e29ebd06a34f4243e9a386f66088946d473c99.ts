import { UserValidationsError } from "@/lib/CustomErrors";

export interface IUserDTO {
  name: string;
  email: string;
  password: string;
}

export default class User {
  constructor(
    private readonly name: string,
    private readonly email: string,
    private readonly password: string,
    private readonly passwordConfirmation: string,
  ) {}

  getUser(): IUserDTO {
    let user;

    if (this.nameValidation() && this.emailValidation() && this.passwordValidation()) {
      user = {
        name: this.name,
        email: this.email,
        password: this.password,
      };
    }

    return user!;
  }

  private nameValidation() {
    if (this.name.length < 3 || this.name.trim().length === 0) {
      throw new UserValidationsError(
        "Nome inválido.",
        "O nome deve conter 3 ou mais caracteres e não pode ser em branco.",
        "O nome deve conter 3 ou mais caracteres.",
        400,
        true,
      );
    }

    return true;
  }

  private emailValidation() {
    const emailRegex =
      /[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?/g;

    if (!emailRegex.test(this.email)) {
      throw new UserValidationsError(
        "Email inválido.",
        "Formato de email inválido.",
        "Insira um email válido.",
        400,
        true,
      );
    }

    return true;
  }

  private passwordValidation() {
    const passwordRegex = /^(?=.*[A-Z])(?=.*\d)(?=.*[!@#$%^&*()_+\-=[\]{};':"\\|,.<>\/?]).{8,}$/;

    if (!passwordRegex.test(this.password)) {
      throw new UserValidationsError(
        "Formato de senha inválida.",
        "A senha não segue o seguinte formato: 8 Caracteres, 1 Letra maiúscula, 1 Número e 1 Caractere especial.",
        "A senha deve conter: 8 Caracteres, 1 Letra maiúscula, 1 Número e 1 Caractere especial.",
        400,
        true,
      );
    }

    if (this.password !== this.passwordConfirmation) {
      throw new UserValidationsError(
        "As senhas não são iguais.",
        "As senhas não são iguais.",
        "Verifique as senhas informadas.",
        400,
        true,
      );
    }

    return true;
  }
}