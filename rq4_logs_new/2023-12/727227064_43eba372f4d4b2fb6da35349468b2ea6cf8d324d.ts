import { Injectable, UnauthorizedException } from "@nestjs/common";
import { JwtService } from "@nestjs/jwt";
import { PrismaService } from "src/prisma/prisma.service";

@Injectable()
export class AuthService {
  constructor(
    private readonly jwtService: JwtService,
    private readonly prisma: PrismaService
  ) { }

  async createToken() {
    // return this.jwtService.sign();
  }

  async checkToken(token: string) {
    // return this.jwtService.verify(token);
  }

  async login(email: string, password: string) {
    const user = await this.prisma.user.findFirst({
      where: {
        email,
        password
      }
    });

    if (!user) {
      throw new UnauthorizedException('E-mail and/or password incorrect');
    }

    return user;
  }

  async forget(email: string) {
    const user = await this.prisma.user.findFirst({
      where: {
        email
      }
    });

    if (!user) {
      throw new UnauthorizedException('E-mail is incorrect');
    }

    //TO DO: Enviar o email para processo de troca de senha.

    return true;
  }

  async reset(password: string, token: string) {
    // To Do: validar o token
    const id = 0;

    await this.prisma.user.update({
      where: {
        id
      },
      data: {
        password
      },
    });

    return true;
  }
}