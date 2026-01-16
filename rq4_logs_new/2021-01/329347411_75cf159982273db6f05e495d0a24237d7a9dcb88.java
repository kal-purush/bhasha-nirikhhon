package homeWork.fifthLesson;

public class Main {
    public static void main(String[] args) {
        Worker[] workers = new Worker[5];
        workers[0] = new Worker("Vladislav familia", "Programmer", "v@mail.com", "1234", 35000, 24);
        workers[1] = new Worker("Semion familia", "Kasir", "s@mail.com", "4321", 50000, 48);
        workers[2] = new Worker("Valentina familia", "Bugalter", "v@mail.ru", "5387", 58000, 42);
        workers[3] = new Worker("Slavik familia", "Mehanik", "sl@mail.com", "84662", 72000, 41);
        workers[4] = new Worker("Marin familia", "Praktikant", "m@mail.com", "55321", 43000, 21);

        for (int i = 0; i < workers.length; i++) {
            if (workers[i].getAge() >= 40) {
               workers[i].printInformation();
                System.out.println();
            }
        }
    }
}