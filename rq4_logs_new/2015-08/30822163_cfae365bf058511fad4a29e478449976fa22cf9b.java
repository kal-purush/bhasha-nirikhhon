package motocitizen.network.requests;

/**
 * Created by U_60A9 on 31.08.2015.
 */
public enum Methods {
    AUTH, GET_LIST, BAN, ONWAY, CANCEL_ONWAY, INPLACE, LEAVE, MESSAGE, CREATE, REGISTER_GCM, CHANGE_STATE;
    final static String auth        = "auth";
    final static String getList     = "getlist";
    final static String ban         = "ban";
    final static String onway       = "onway";
    final static String cancelOnway = "cancelOnWay";
    final static String inplace     = "inplace";
    final static String leave       = "leave";
    final static String message     = "message";
    final static String create      = "createAcc";
    final static String registerGCM = "registerGCM";
    final static String changeState = "changeState";

    final static String unknown = "";

    public String toCode() {
        switch (this) {
            case AUTH:
                return auth;
            case GET_LIST:
                return getList;
            case BAN:
                return ban;
            case ONWAY:
                return onway;
            case CANCEL_ONWAY:
                return cancelOnway;
            case INPLACE:
                return inplace;
            case LEAVE:
                return leave;
            case MESSAGE:
                return message;
            case CREATE:
                return create;
            case REGISTER_GCM:
                return registerGCM;
            case CHANGE_STATE:
                return changeState;
            default:
                return unknown;

        }
    }
}