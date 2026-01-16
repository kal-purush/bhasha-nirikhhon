import { LaravelApi } from "backend/ApiTypesObjects/ApiTypeLaravel";
import { httpController } from "backend/controllers/htppController";


export class postLogoutService {
    execute( token: string ) {
        const http = new httpController();

        const config = {
            headers: {
                'Content-type': 'application/json; charset=UTF-8',
                'Accept': 'application/json',
                Authorization: `Bearer ${token}`
            }
        }

        return http
        .handle( new LaravelApi() )
        .post("/logout", {}, config);
    }
}