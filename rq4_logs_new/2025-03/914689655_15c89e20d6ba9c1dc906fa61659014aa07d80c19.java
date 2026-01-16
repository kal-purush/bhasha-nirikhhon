package DataAccess;

import model.AuthData;

import java.util.HashMap;

public class MemoryAuthDAO implements AuthDAO{
    private final HashMap<String, AuthData> authMap;

    public MemoryAuthDAO(HashMap<String, AuthData> authMap) {
        this.authMap = authMap;
    }

    public void clearAllAuth(){
        authMap.clear();
        //clear database of authData;
    }
    public AuthData getAuth(){

        return null;
        //Returns authData from database
    }
    public void removeAuth(){
        //removes one individual's authData
    }
    public void addAuth(AuthData authData){
        // adds authData to database
    }
}