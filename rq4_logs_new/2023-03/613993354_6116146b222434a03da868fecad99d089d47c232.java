package by.ekids.museum;

public class MuseumPrinter {

    public static final String pattern = "\nМузей работает по %d направлениям"; //%d - числа
    private static Museum museum;

    public static void setMuseum(Museum aMuseum) {
        museum = aMuseum;
    }

    public static void printInfo() {
        System.out.println("музей - " + museum.getName());
        System.out.println("c размером коллекции: " + museum.getCollectionSize() + " экземпляров");
        System.out.println("и тематикой: ");

        String[] departments = museum.getDepartments();

        try {
            for (int i = 0; i < departments.length + 1; i++) {
                System.out.print(departments[i] + " ");
            }
        } catch (Exception e) {
            System.out.println("Ошибка доступа");
        }


        int departmentsCount = departments.length;
        switch (departmentsCount) {
            case 1 -> System.out.printf(pattern, 1);
            case 2 -> System.out.printf(pattern, 2);
            case 3 -> System.out.printf(pattern, 3);
        }

//        switch (departmentsCount) {
//            case 1: {
//                System.out.println("\nМузей работает по 1 направлению");
//                break;
//            }
//            case 2:
//            case 3: {
//                System.out.println("\nМузей работает по нескольким направлению");
//                break;
//            }
//        }

        System.out.println("\nВыходной: " + museum.getDayOff().getName());
    }


}