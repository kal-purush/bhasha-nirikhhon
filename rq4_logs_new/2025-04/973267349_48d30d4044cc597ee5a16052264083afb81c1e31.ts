import { IsEmail, IsNotEmpty, IsOptional, IsString, IsStrongPassword } from "class-validator";
import { isEmailRegistered } from "../validations/validarEmail";

export class AtualizaUsuarioDTO {
    @IsString({ message: 'O nome deve ser uma string' })
    @IsNotEmpty({ message: 'O nome não pode ser vazio' })
    @IsOptional()
    nome: string;

    @IsEmail(undefined, { message: 'O email deve ser um email válido' })
    @isEmailRegistered({ message: 'O email já está cadastrado' })
    @IsOptional()
    email: string;

    @IsString({ message: 'A senha deve ser uma string' })
    @IsStrongPassword(undefined, { message: 'Senha fraca' })
    @IsOptional()
    senha: string;
}