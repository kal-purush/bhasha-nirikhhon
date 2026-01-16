package InClassOOP;

public class DishTools {
    public static final int AVERAGE_COST_OF_DISH_IN_CENTS = 1300;

    public static void shoutDishName(Dish d) {
        System.out.println(d.nameOfDish.toUpperCase());
    }

    public static void analyzeDishCost(Dish d) {
        if (d.costInCents > AVERAGE_COST_OF_DISH_IN_CENTS) {
            System.out.println("More expensive than average.");
        } else if (d.costInCents < AVERAGE_COST_OF_DISH_IN_CENTS) {
            System.out.println("Less expensive than average.");
        } else {
            System.out.println("Same as average.");
        }
    }

    public static void flipRecommendation(Dish d) {
        d.wouldRecommend = !d.wouldRecommend;
        System.out.println("Recommendation has changed to: " + d.wouldRecommend);
    }
}