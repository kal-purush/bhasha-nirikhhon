type UserInputType = {
  email: string,
  password: string,
  name: string,
  phone: string,
  address: string,
  type: string,
  extra: extraType,
}

type extraType = {
    X_position: string
    Y_position: string
  }


// eslint-disable-next-line @typescript-eslint/no-unused-vars
class CustomerInput implements UserInputType {
    email: string;
    password: string;
    name: string;
    phone: string;
    address: string;
    type: string;
    extra: extraType;
  
    constructor(){
      this.email = ""
      this.password = ""
      this.name = ""
      this.phone = ""
      this.address = ""
      this.type = "user"
      this.extra = {
        X_position: "",
        Y_position: ""
      }
    }
  }

  interface AuthSlice {
    signUp: (UserInput: UserInputType) => void;
    // logIn: 
    // 인증
    // 인가
  }
  