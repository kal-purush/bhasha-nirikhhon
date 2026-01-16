public class EqualsDiff {
    public static void main(String[] args) {
        String str1 = "Dev";
        String str2 = "Dev";
        String str3 = "Ved";

        String strobj1 = new String("Dev");
        String strobj2 = new String("Dev");
        String strobj3 = new String("Ved");

        System.out.println("str1 == str2: " +(str1 == str2)); //true
        System.out.println("str1 != str2: "+(str1 != str2)); //false
        System.out.println("strobj1 == strobj2: "+(strobj1 == strobj2));//true
        System.out.println("strobj1 != strobj2: "+(strobj1 != strobj2));//false
        System.out.println("str1.equals(str2): "+(str1.equals(str2)));//true
        System.out.println("strobj1.equals(strobj2): "+strobj1.equals(strobj2));//true
        System.out.println("str1.equals(strobj1): "+(str1.equals(strobj1)));
        System.out.println("str1==str3: "+(str1==str3));
        System.out.println("str1.equals(str3): "+(str1.equals(str3)));
        System.out.println("strobj1==strobj3: "+(strobj1==strobj3));
        System.out.println("strobj1.equals(strobj3): "+(strobj1.equals(strobj3)));
        System.out.println("strobj1 != strobj3: "+(strobj1 != strobj3));
    }
}