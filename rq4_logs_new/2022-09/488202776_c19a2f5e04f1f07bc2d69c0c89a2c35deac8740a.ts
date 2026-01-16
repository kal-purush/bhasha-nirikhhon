import { USER_ROLES } from './../models/User';
import { Authenticator, ITokenPayload } from './../services/Authenticator';
import { HashManager } from './../services/HashManager';
import { IdGenerator } from "../services/IdGenerator"
import { User } from '../models/User';
import { UserDatabase } from '../database/UserDatabase';

export class UserBusiness {
    public singup = async (
        name:string,
        email:string,
        password:string
        ) =>{
            if(!name|| typeof name !== "string"){
                throw new Error("Parâmetro 'name' invalido")
            }
            if(!email|| typeof email !== "string"){
                throw new Error("Parâmetro 'email' invalido")
            }
            if(!password|| typeof password !== "string"){
                throw new Error("Parâmetro 'password' invalido")
            }

            const generateId= new IdGenerator()
            
            const id = generateId.generate()
            
            const hashManager =new HashManager()

            const hash = await hashManager.hash(password)

            const user = new User(
                id,
                name,
                email,
                hash
                )

            const userDataBase = new UserDatabase ()

            await userDataBase.creatUser(user)

            const authenticator = new Authenticator()

            const payLoad: ITokenPayload = {
                id:user.getId(),
                role:user.getRole()
            }

            const token = authenticator.generateToken(payLoad)

            const response = {
                token
            }

            return response

    }

    public login =async (email:string,password:string) => {
        
        const userDatabase= new UserDatabase()

        const newUser = await userDatabase.getUserByEmail(email)

        const user= new User(
            newUser.id,
            newUser.name,
            newUser.email,
            newUser.password,
            newUser.role)

            const payLoad: ITokenPayload = {
                id:user.getId(),
                role:user.getRole()
            }

        const authenticator = new Authenticator()
            
        const token = authenticator.generateToken(payLoad)

        const hashManager =new HashManager()

        const compare = await hashManager.compare(password,user.getPassword())

        if(compare){
            return token
            }else{
                throw new Error("Email ou senha incorreta")
            }
    }

    public allUser = async (token:string) => {

        const authenticator = new Authenticator()

        const auth = authenticator.getTokenPayload(token)

        if(auth.role !== USER_ROLES.ADMIN ){
            throw new Error("Não autorizado")
        }

        const userDatabase= new UserDatabase()

        const allUsers = await userDatabase.allUsers()

        return allUsers

    }

    public delete = async (token:string,id:string) =>{

        const authenticator = new Authenticator()

        const auth = authenticator.getTokenPayload(token)

        if(auth.role !== USER_ROLES.ADMIN ){
            throw new Error("Não autorizado")
        }

        const userDatabase= new UserDatabase()

        const response =await userDatabase.deleteUsers(id)

        return response



    }


}