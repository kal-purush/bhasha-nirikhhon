import {Container} from "inversify";
import "reflect-metadata";
import {TYPES} from "./types";
import {ISigner} from "../../../modules/identityUser/services/Authorization/interfaces/ISigner";
import {JWTSigner} from "../../../modules/identityUser/services/Authorization/JWTSigner";
import {IIdentityUserRepo} from "../../../modules/identityUser/repos/interfaces/IIdentityUserRepo";
import {IdentityUserRepo} from "../../../modules/identityUser/repos/IdentityUserRepo";
import {secret, signOptions, verifyOptions} from "../../jwt/config/config";
import * as models from "../../sequlize/models"
import {UseCase} from "../../../core/domain/UseCase";
import {RegisterUseCase, RegisterUseCaseResponse} from "../../../modules/identityUser/useCases/register/RegisterUseCase"
import {RegisterDTO} from "../../../modules/identityUser/useCases/register/RegisterDTO";
import {AuthDTO} from "../../../modules/identityUser/useCases/identityAuth/AuthDTO";
import {AuthResponse, AuthUseCase} from "../../../modules/identityUser/useCases/identityAuth/AuthUseCase";
import {IdentityUser} from "../../../modules/identityUser/domain/IdentityUser";
import {Result} from "../../../core/logic/Result";
import {IdentityTokenDTO} from "../../../modules/identityUser/useCases/identityAuth/IdentityTokenDTO";
import {IdentityTokenUseCase} from "../../../modules/identityUser/useCases/identityAuth/IdentityTokenUseCase";

const container = new Container();
container.bind<ISigner>(TYPES.ISigner).toDynamicValue(() => new JWTSigner(secret, verifyOptions, signOptions)).inSingletonScope();
container.bind<IIdentityUserRepo>(TYPES.IIdentityUserRepo).toDynamicValue(() => new IdentityUserRepo(models)).inTransientScope();
container.bind<UseCase<RegisterDTO, Promise<RegisterUseCaseResponse>>>(TYPES.RegisterUseCase).to(RegisterUseCase).inTransientScope();
container.bind<UseCase<AuthDTO, Promise<AuthResponse>>>(TYPES.AuthUseCase).to(AuthUseCase).inTransientScope();
container.bind<UseCase<IdentityUser, Promise<Result<IdentityTokenDTO>>>>(TYPES.IdentityTokenUseCase).to(IdentityTokenUseCase).inSingletonScope();

export default container;