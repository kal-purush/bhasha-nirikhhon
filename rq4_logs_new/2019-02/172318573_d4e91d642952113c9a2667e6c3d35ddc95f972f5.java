public class Printer {

    public static String printerError(String s) {
        int length = s.length();
        long errorNumber = s.chars().filter(asci -> asci > 109 && asci <= 122).count();
        return errorNumber+"/"+length;
    }

}