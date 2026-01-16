import { UnauthorizedException } from "@nestjs/common";
import Client from "../client.model";
import createClient from "../../products/Client/createClient.model";
import deactivateClient from "../../products/Client/deactivateClient.model";



describe("bankManager create products tests",()=>{
    const account: Client  = new Client()
    test("should return an createClient object", () =>{
        expect(account.createAccount()).toBeInstanceOf(createClient)
    })
    test("should return an deactivateClient object", () => {
        expect(account.deactivateAccount()).toBeInstanceOf(deactivateClient)
    })
   
})