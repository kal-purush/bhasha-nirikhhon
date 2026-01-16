 /**
Customer.ts 

This file contain the model for the customer class for vantage 
*/

module Vantage {
    export enum AddressType {
        Billing,
        Shipping,
        Primary

    }
    export interface IPublisher {
        Publish(topic: string, message: any);
    }
    

    export class Address {
        public addressID: string;
        public addresstypecode: AddressType; 
        public country: string = 'USA';
        public line1: string; 
        public line2: string; 
        public line3: string; 
        public postalCode: string; 
    }
    export class Customer {
        public ID: number; 
        public firstName: string;
        public lastName: string;
        public workPhone: string;
        public homePhone: string;
        public Addresses: Array<Address>; 
        public primaryAddress: Address;      
    }
}