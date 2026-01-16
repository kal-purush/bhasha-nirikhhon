import { DeepPartial, Repository } from 'typeorm';
import { MyPostgresDataSource } from '../config/database';
import { Token } from '../database/entity/Token';

export default class TokenService {
    private tokenRepository: Repository<Token>;

    constructor() {
        this.tokenRepository = MyPostgresDataSource.getRepository(Token);
      }

    public async insertToken(token: Token): Promise<void> {
        await this.tokenRepository.save(token)
    }

    public async getToken(token: Token): Promise<Token> {
        return await this.tokenRepository.findOne(
            {
                where: { token }
            }
        )
    }


}