import { photographySignUpRequest } from "../../interfaces/photographySignUpRequest";
import apiClient from "../apiClient";

export const photographySignUp = async({username, password, email, phoneNumber, photographyCompanyName}:photographySignUpRequest) => {
    await apiClient.post('User/CreatePhotographyCompany',{
        username, 
        password, 
        email, 
        phoneNumber, 
        photographyCompanyName
    },{
    headers: {
      'Content-Type': 'application/json'
    }
  })
}