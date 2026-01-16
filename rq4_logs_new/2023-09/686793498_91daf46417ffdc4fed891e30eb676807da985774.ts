import { ISessionUser, user_home, user_payment } from "hive-link-common";

export default class User implements ISessionUser {
    id: ISessionUser["id"];
    email: ISessionUser["email"];
    first_name: ISessionUser["first_name"];
    last_name: ISessionUser["last_name"];
    role: ISessionUser["role"];
    

    constructor(user: ISessionUser) {
        this.id = user.id;
        this.email = user.email;
        this.first_name = user.first_name;
        this.last_name = user.last_name;
        this.role = user.role;
    }

    getHomes: () => user_home[] = () => {
        
        return [];
    }


}