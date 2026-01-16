import * as axios from "axios";
import { response } from "express";


export class Notifications{

    private constructor() {}

    public static notify(clientId,subject,message){

        axios.default
        .post("https://fnbsim.southafricanorth.cloudapp.azure.com/notify", {
        clientID : clientId,
        subject : subject,
        message : message
      },{
          headers : {
            "Content-Type" : 'application/json'
          }
      }). then( (res) =>{
          console.log("Notification Success - ",res.status," ",res.data);
      }).catch(err => {
        console.error("Notification Error - ",err.status," ",err.data);
      });
    }
}