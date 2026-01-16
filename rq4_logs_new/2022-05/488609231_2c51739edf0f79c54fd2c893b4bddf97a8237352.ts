type Address = {
    line1: string;
    line2: string;
    zipCode: string;
    city: string;
    country: string;
};

export default class User {
    constructor(
        public firstName: string,
        public lastName: string,
        public email: string,
        public password: string,
        public phone: string,
        public address: Address,
    ) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.password = password;
        this.phone = phone;
        this.address = address;
    }
}