package chapter02.item02;

public class BuilderTest {
    public static void main(String[] args) {
        NutritionFacts_Builder cocaCola = new NutritionFacts_Builder.Builder(240, 8).calories(100).fat(10).build();

        System.out.println(cocaCola.toString()); // NutritionFacts_Builder{servingSize=240, servings=8, calories=100, fat=10}
    }
}