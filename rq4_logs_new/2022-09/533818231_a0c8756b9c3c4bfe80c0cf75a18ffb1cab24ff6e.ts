import ejs from 'ejs'
import mssql from 'mssql'
import dotenv from 'dotenv'
import {sqlconfig} from '../config/config'
import sendMail from '../Helpers/Email';
import Connection from '../Helpers/database'
const db = new Connection();
dotenv.config()

interface Parcel{
    id:number,
    senderEmail:string,
    receiverEmail:string,
    trackId:string,
    location:string,
    destination:string,
    dispatchedDate:string,
    weight:number,
    price:number,
    markers:string,
    status:number,
    deleted:number
}


const DeliveredEmail= async()=>{

const parcels: Parcel[]= await (await db.exec('checkParcel')).recordset


const emails:string[] = []




 for(let parcel of parcels){

    ejs.renderFile('templates/deliveredparcel.ejs',{trackId:parcel.trackId} ,async(error,data)=>{

        let messageoption={
            from:process.env.EMAIL,
            to:parcel.receiverEmail,
            subject:`Your Parcel ${parcel.trackId} has Been Delivered`,
            html:data,
            attachments:[
                {
                    filename:'parcel.txt',
                    content:`Order summary for a package from : ${parcel.trackId}`
                }
            ]
        }




       
        

        try {
            
            await sendMail(messageoption)
            await  db.exec('updateParcelStatus')
            
            
        } catch (error) {
            console.log(error);
            
        }


    })


    ejs.renderFile('templates/deliveredparcel.ejs',{trackId:parcel.trackId} ,async(error,data)=>{

        let messageoption={
            from:process.env.EMAIL,
            to:parcel.senderEmail,
            subject:`Your Parcel ${parcel.trackId} has Been Delivered`,
            html:data,
            attachments:[
                {
                    filename:'parcel.txt',
                    content:`Order summary for a package from : ${parcel.trackId}`
                }
            ]
        }




       
        

        try {
            
            await sendMail(messageoption)
            await  db.exec('updateParcelStatus')
            
            
        } catch (error) {
            console.log(error);
            
        }


    })


 }


}

export default DeliveredEmail