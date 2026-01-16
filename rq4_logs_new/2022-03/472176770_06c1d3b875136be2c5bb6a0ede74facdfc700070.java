package Session4.lab1;

public class Main {
    public static void main(String[] args) {
        PhoneBook pb = new PhoneBook();
        pb.insertPhone("Abc", "01234578");
        pb.insertPhone("Thuong2", "012345");
        pb.insertPhone("Thuong", "0967940576");
        pb.insertPhone("Thuong2", "0123456789");

//        for (PhoneNumber pn: pb.PhoneList){
//            System.out.println(pn.name);
//            System.out.println(pn.phone);
//        }

//        pb.removePhone("Thuong");
//        for (PhoneNumber pn: pb.PhoneList){
//            System.out.println(pn.name);
//            System.out.println(pn.phone);
//        }
//        pb.updatePhone("Thuong2", "036548");
//        for (PhoneNumber pn: pb.PhoneList){
//            System.out.println(pn.name);
//            System.out.println(pn.phone);
//        }
//        pb.searchPhone("Tu");
//        for (PhoneNumber pn: pb.PhoneList){
//            System.out.println(pn.name);
//            System.out.println(pn.phone);
//        }
        pb.sort();

    }
}

