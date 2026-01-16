interface AUser extends Document{
    email: string, 
    firstname: string, 
    lastname: string,
    password: string, 
    fullname: string, 
    _doc: any
}

interface UserLogin{
    email: string, 
    password: string
}


export {
    AUser, 
    UserLogin
}