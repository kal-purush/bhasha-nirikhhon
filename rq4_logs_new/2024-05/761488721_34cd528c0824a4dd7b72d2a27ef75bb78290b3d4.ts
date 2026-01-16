import { SetStateAction, useState } from "react";
import { userValidator } from "../../Service/Validators/userValidator";
import { userService } from "../../Service/API/userServices";
import { StateAndSetters } from "../../utils/types/Interfaces/StateAndSetters";
import { User } from "../../Service/Entities/userEntities";

const RedefinirStateController = () => {
  const [ redefinirSenha, setRedefinirSenha ] = useState('')
  const [password, setPassword] = useState("");
  const [confirmarSenha, setConfirmarSenha] = useState("");

  const validator: userValidator = new userValidator();
  const UserService: userService = new userService();

  const setState: StateAndSetters = {
      redefinirSenha: setRedefinirSenha,
      password: setPassword,
      confirmarSenha: setConfirmarSenha,
      nome: function (value: SetStateAction<string>): void {
          throw new Error("Function not implemented.");
      },
      sobrenome: function (value: SetStateAction<string>): void {
          throw new Error("Function not implemented.");
      },
      email: function (value: SetStateAction<string>): void {
          throw new Error("Function not implemented.");
      }
  };

  const handleFieldChange = async (field: string, value: string): Promise<{ valido: boolean, value: number, error?: string | Error}> => {
    console.log(`validando ${field} ...`);
    if (field in setState) {
      setState[field as keyof StateAndSetters](value);
      const valfield = await validator.valByField(field, value);
      if (valfield.valido === false) {
        console.log(valfield.erro);
        return { valido: true, value: 401, error: valfield.erro}
      }
      console.log("validação concluída");
      return { valido: true, value: 200}
    } 
      console.error(
        `Campo "${field}" não é uma chave válida em StateAndSetters.`
      );
      return { valido: false, value: 400, error: `Campo "${field}" não é uma chave válida em StateAndSetters.`}
    
  };

  const handleConfirmarSenhaChange = async (
    senha: string,
    confirmsenha: string
  ): Promise<{ valido: boolean, value: number, error?: string | Error}>=> {
    setConfirmarSenha(confirmsenha);
    const valconf = validator.confirmarSenha(senha, confirmsenha);
    if (valconf.valido === false) {
      console.log("As senhas não coincidem");
      return { valido: false,  value: 401, error: valconf.erro };
    }
    console.log("validação concluída");
    return { valido: true, value: 200 };
  };

  const handleResetRequest = async (
    email: string,
    password: string
  ): Promise<{
    valido: boolean;
    value?: number;
    error?: string | Error;
    data?: string;
  }> => {
    try {
      const req = await UserService.resetPwd(email, password);
      if (req.valido === false) {
        throw new Error("Bad Request");
      }
      return { valido: true, value: 201, data: req.data };
    } catch (error) {
      if (error instanceof Error) {
        if (error.message === "Unauthorized") {
          return { valido: false, value: 401, error: error };
        } else if (error.message === "Bad Request") {
          return { valido: false, value: 400, error: error };
        }
      }
      return { valido: false, value: 500, error: "Internal Server Error" };
    }
  };

  const handleRedefinir =  async (email: string, password: string, confirmarSenha: string): Promise<{
    valido: boolean;
    value?: number;
    error?: string | Error;
    data?: User;
  }>  => {
    try {
      const req = await UserService.update(email, 'password', password)

    if (req.valido === false) {
        throw new Error("Bad Request");
      }
      return { valido: true, value: 201, data: req.data };
    } catch (error) {
      if (error instanceof Error) {
        if (error.message === "Unauthorized") {
          return { valido: false, value: 401, error: error };
        } else if (error.message === "Bad Request") {
          return { valido: false, value: 400, error: error };
        }
      }
      return { valido: false, value: 500, error: "Internal Server Error" };
    }
  }

  return {
    password,
    confirmarSenha,
    handleFieldChange,
    handleConfirmarSenhaChange,
    handleResetRequest,
    handleRedefinir
  };
};

export { RedefinirStateController };