package com.leechangu.sweettask;

/**
 * Created by Administrator on 2015/11/25.
 */
public class UserMng {

    private static UserMng ourInstance = new UserMng();
    private String myUsername;
    private String partnerUsername;

    private UserMng() {
    }

    public static UserMng getInstance() {
        return ourInstance;
    }

    public String getMyUsername() {
        return myUsername;
    }

    public void setMyUsername(String myUsername) {
        this.myUsername = myUsername;
        partnerUsername = getPartnerNameFromParse();
    }

    public String getPartnerUsername() {
        return partnerUsername;
    }

    private String getPartnerNameFromParse() {
        // mock method
        // UserMngRepository.get
        return "111";
    }
}