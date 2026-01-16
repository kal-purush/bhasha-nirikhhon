package LAB_PBO_2024.Praktikum.Praktikum_03.Nomor_1;

public class Main {
    public static void main(String[] args) {
        Move move1 = new Move();
        move1.setName("Dragon Uppercut");
        move1.setDamage(30);

        Tekken fighter1 = new Tekken("Kazuya Mishima", move1.getName(), move1.getDamage(), 1000, 1.0);

        Move move2 = new Move("Lightning Bolt", 25);

        Tekken fighter5 = new Tekken();
        System.out.println(fighter5.getHealth());

        Tekken fighter2 = new Tekken();
        fighter2.setName("Jin Kazama");
        fighter2.setMove(move2);
        fighter2.setHealth(1000);
        fighter2.setPositions(2.5);

        printBarriers();
        fighter1.checkStatus();
        printBarriers();
        fighter2.checkStatus();

        printBarriers();
        fighter1.attack(fighter2);

        printBarriers();
        fighter1.Bergerak(2.5);

        printBarriers();
        fighter1.attack(fighter2);

        printBarriers();
        fighter1.checkStatus();
        printBarriers();
        fighter2.checkStatus();

        printBarriers();
        fighter2.upgradeMove("Devil Beam", 40);

        printBarriers();
        fighter2.attack(fighter1);

        printBarriers();
        fighter1.checkStatus();
        printBarriers();
        fighter2.checkStatus();

        printBarriers();
        fighter2.Bergerak(1.5);

        printBarriers();
    }

    public static void printBarriers() {
        System.out.println("================================");
    }
}