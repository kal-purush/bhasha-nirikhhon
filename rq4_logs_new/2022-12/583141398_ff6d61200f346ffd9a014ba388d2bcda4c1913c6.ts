import { compareSync } from "bcrypt";
import { AppDataSource } from "../../data-source";
import { User } from "../../entities/user";
import { AppError } from "../../errors/appError";
import { ILoginRequest } from "../../interfaces/user";
import jwt from 'jsonwebtoken';


export const loginService = async (userData: ILoginRequest)  => {
    let checkKeys = Object.keys(userData).map(
        item => item.includes('email') || item.includes('password')
    )

    if(checkKeys.includes(false)){
        throw new AppError('Invalid Key')
    }

    const userRepository = AppDataSource.getRepository(User)

    const user = await userRepository.findOneBy({email: userData.email})

    if(!user){
        throw new AppError('Invalid user or password', 403)
    }

    const checkPass = compareSync(userData.password, user.password)

    if(!checkPass){
        throw new AppError('Invalid user or password', 403)
    }

    const token = jwt.sign(
        {
            userId: user.id,
            userRole: user.role
        },
        process.env.SECRET_KEY as string,
        {
            expiresIn: '24h',
       }
    );

    return token
}