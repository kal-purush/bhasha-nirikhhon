import { Injectable, NotFoundException } from '@nestjs/common';
import { UsersService } from '../users/users.service';
import { User, UserDocument } from '../schemas/user.schema';
import { Model } from 'mongoose';
import { InjectModel } from '@nestjs/mongoose';

@Injectable()
export class ProfilService {
    constructor(
        private usersServices: UsersService,
        @InjectModel(User.name) private userModel: Model<UserDocument>,
    ) {}

    async updateProfil(id: string, user: User) {
        const userExist = await this.usersServices.findOneById(id);
        if (!userExist) {
            throw new NotFoundException("L'utilisateur n'existe pas");
        }
        return this.usersServices.updateOneById(id, user);
    }

    async updateProfilePicture(id: string) {
        const userExist = await this.usersServices.findOneById(id);
        if (!userExist) {
            throw new NotFoundException("L'utilisateur n'existe pas");
        }
        return '';
    }
}