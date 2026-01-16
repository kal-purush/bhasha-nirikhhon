package server.handlers.services;

public class UserServices {
    //UserHandler will refer to this regardless of method, this is just to make it nice and separate

    //Call UserDAO.getUser, if it returns null, create authToken and send UserData model to UserDAO.createUser
    public Object checkUsername(){
        //Check username to make sure that it is available
        return null;
    }

    public Object createUser(){
        //checkUsername();
        //create user model
        //call UserDAO.addUser(new user model)
        //generate authToken with CreateAuth.newToken()
        //create auth Model
        //call AuthDAO.addAuth(new auth model)
        return null;

    }
}