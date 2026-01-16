export interface Entity {
  _id : string;
}

export interface CouchDBEntity extends Entity {
  _rev : string;
  type : string;
}

export interface Contact extends CouchDBEntity {

  firstName : string;
  lastName  : string;

  birthday  : Date;
  phoneNumbers:PhoneNumber[];
  emailAddresses:EmailAddress[];
  addresses:Address[];

  imageUrl : string;
}

export interface PhoneNumber {
  label : string;
  number: string;
}

export interface EmailAddress {
  label : string;
  address : string;
}

export interface Address {
  label : string;
  street : string;
  zipcode : string;
  city : string;
  country : string;
}