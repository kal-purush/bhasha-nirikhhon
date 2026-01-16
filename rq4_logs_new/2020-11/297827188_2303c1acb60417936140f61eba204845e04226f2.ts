import { UserProfile } from '../models/user-profile/user-profile';
export class Accounts {
   
     
	 id:number;
	 user:UserProfile;
    accountType:number;
	 balance:any;
	 status:number;
    createdDate:Date;
    lastUpdatedDate:Date;

}