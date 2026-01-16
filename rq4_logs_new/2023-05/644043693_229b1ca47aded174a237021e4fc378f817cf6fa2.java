public class Farmer {
    private int seeds, carrots;
    private double money;

    public Farmer(double given_moneys, int given_seeds, int given_carrots) {
        money = given_moneys;
        seeds = given_seeds;
        carrots = given_carrots;
    }

//    int getMoney() {
//        return money;
//    }
//
//    int getSeeds() {
//        return seeds;
//    }
//
//    int getCarrots() {
//        return carrots;
//    }

    void buy_seeds(double seed_purchase_price) {
        while (money > seed_purchase_price) {
            money -= seed_purchase_price;
            seeds += 1;
        }
    }

    public void show_off() {
        System.out.println("Hello I am Farmer!");
        System.out.println("money: " + money);
        System.out.println("seeds: " + seeds);
        System.out.println("carrots: " + carrots);
    }


}
