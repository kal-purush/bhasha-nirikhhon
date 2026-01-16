import { UndefinedException } from '../../../Shared/domain/exceptions/UndefinedException';
import Logger from '../../../Shared/domain/Logger';
import { TokenRepository } from '../../../Shared/domain/TokenRepository';
import { Uuid } from '../../../Shared/domain/value-object/Uuid';
import { LoginManager } from '../domain/LoginManager';
import { UserPetition } from '../domain/User/UserPetition';
import { UserType } from '../domain/User/UserType';
import * as jwt from "jsonwebtoken";

export class JWTLogin implements LoginManager{
    constructor(private logger:Logger, private tokenRepository:TokenRepository) {
    }
    public login(userId: Uuid, userType: UserType): string {
        if (process.env.ACCESS_TOKEN_SECRET && process.env.REFRESH_TOKEN_SECRET){
            const jwtToken = jwt.sign(new UserPetition(userId, userType), process.env.ACCESS_TOKEN_SECRET, { expiresIn: '15s' });
            const refreshToken = jwt.sign(new UserPetition(userId, userType), process.env.REFRESH_TOKEN_SECRET);
            this.tokenRepository.createToken(userId.value, refreshToken);
            this.logger.info("User with id: " + userId.value + " has created a token with value " + jwtToken + " and refresh token with value " + refreshToken);
            return jwtToken + " " + refreshToken;
        }
        throw new UndefinedException("Key undefined");
    }

    public logout(userId:Uuid): void {
        this.tokenRepository.deleteToken(userId.value);
    }

}