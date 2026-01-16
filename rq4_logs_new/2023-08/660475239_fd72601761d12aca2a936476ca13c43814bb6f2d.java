package day16arraylists;

public class PassByValuePassByReference {
    public static void main(String[] args) {

        int shirtPrice = 100;
        student(shirtPrice,10);
        String result =putExclamation("Ali");
        System.out.println(result);
    }
        public static void student(int shirtPrice,int discount)
        {
            int discountedPrice = shirtPrice-discount;
            System.out.println(discountedPrice);
        }

        public static String putExclamation(String name)
        {
        return name+"!";
        }


}