import { InternalServerError, ResetPasswordServiceError } from "@/lib/CustomErrors";
import getBaseUrl from "@/lib/getBaseUrl";
import { IHasher } from "@/lib/Hasher";
import { IMailer } from "@/lib/Mailer";
import { ITokenService } from "@/lib/TokenService";
import { IResetPasswordRepository } from "@/repository/ResetPasswordRepository";
import { IUserRepository } from "@/repository/UserRepository";

export default class ResetPasswordService {
  constructor(
    private readonly userRepository: IUserRepository,
    private readonly resetPasswordRepository: IResetPasswordRepository,
    private readonly tokenService: ITokenService,
    private readonly hasher: IHasher,
    private readonly mailer: IMailer,
  ) {}

  async requestResetPassword(email: string) {
    try {
      const isUserExists = await this.userRepository.findUserByEmail(email);
      if (!isUserExists) {
        throw new ResetPasswordServiceError("Usuário não encontrado.", "Verifique suas credenciais.", 404, true);
      }

      const isResetPasswordTokenExists = await this.resetPasswordRepository.findResetPasswordToken(isUserExists.id);
      if (isResetPasswordTokenExists) {
        throw new ResetPasswordServiceError("Email já Enviado.", "Verifique sua caixa de entrada.", 200, true);
      }

      const resetPasswordToken = await this.tokenService.generate({
        resetPasswordToken: Math.random().toString(36).substring(2),
      });
      const hashedResetPasswordToken = await this.hasher.encrypt(resetPasswordToken);
      const expirationTime = new Date(Date.now() + 10 * 60 * 1000);

      await this.resetPasswordRepository.createResetPasswordToken({
        token: hashedResetPasswordToken,
        userId: isUserExists.id,
        expiresAt: expirationTime,
      });

      const resetPasswordUrl = `${getBaseUrl()}/reset-password?token=${resetPasswordToken}&email=${encodeURIComponent(isUserExists.email)}`;

      const emailText = `
      Olá, ${isUserExists.name}.\n
      Para redefirnir sua senha, acesse o link: ${resetPasswordUrl}
    `;

      const emailHTML = `
      <p>Olá, ${isUserExists.name}.</p>
      <br></br>
      <p>Para redefinir sua senha, clique <a href="${resetPasswordUrl}">aqui</a></p>
    `;

      await this.mailer.send(isUserExists.email, "Redefinição de Senha", emailText, emailHTML);
    } catch (error) {
      console.error("Error during reset password request: ", error);

      if (error instanceof ResetPasswordServiceError) {
        throw error;
      }

      throw new InternalServerError(
        "Ocorreu um erro inesperado ao tentar solicitar a redefinição da senha.",
        "Tente novamente mais tarde.",
        500,
        true,
      );
    }
  }
}