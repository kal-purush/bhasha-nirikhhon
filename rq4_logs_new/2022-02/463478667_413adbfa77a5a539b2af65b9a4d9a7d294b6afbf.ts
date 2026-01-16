export class ParentModel{
  fullName:string;
  nicNo:String;
  email:String;
  password:string;
  contactNo1:number;
  contactNo2:number;
  contactNo3:number;


  constructor(fullName: string, nicNo: String, email: String, password: string, contactNo1: number, contactNo2: number, contactNo3: number) {
    this.fullName = fullName;
    this.nicNo = nicNo;
    this.email = email;
    this.password = password;
    this.contactNo1 = contactNo1;
    this.contactNo2 = contactNo2;
    this.contactNo3 = contactNo3;
  }
}