package core;

public enum EcCurve {
    P_224("secp224r1"),
    P_256("secp256r1"),
    P_384("secp384r1"),
    P_521("secp521r1");
    private final String value;
    EcCurve(String value){
        this.value = value;
    }
    public static EcCurve fromValue(String value) throws IllegalArgumentException{
        if(value != null && value.length() > 0) {
            for (EcCurve typ : values()) {
                if (typ.value.equalsIgnoreCase(value)) {
                    return typ;
                }
            }
        }
        throw new IllegalArgumentException("CurveName");
    }
    public String value(){
        return value;
    }
}