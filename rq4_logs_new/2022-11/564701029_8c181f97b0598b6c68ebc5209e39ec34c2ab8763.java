import java.util.Objects;

public class Cat extends Predator{

    private String breed;

    public Cat(String breed, String color, int weight) {
        this.breed = breed;
        this.color = color;
        this.weigth = weight;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Cat)) return false;
        Cat cat = (Cat) o;
        return breed.equals(cat.breed) && color.equals(cat.color) && weigth == cat.weigth;
    }

    @Override
    public int hashCode() {
        return Objects.hash(breed);
    }

    @Override
    public String sleep() {
        return "I'm sleeping. Mrr";
    }

    @Override
    public String hunt() {
        return "I'm hunting. Hold on mice";
    }

    public String tigidik() {
        return "Tigidik-tigidik";
    }

    public String lickBalls() {
        return "Hm...";
    }

    public String occupation(DaysOfWeek daysOfWeek) {
        String occupation = switch (daysOfWeek) {
            case MON, WED, FRI -> sleep();
            case THU, TUE, SAT -> hunt();
            case SUN -> tigidik();
            default -> lickBalls();
        };
        return occupation;
    }

    public static void main(String[] args) {
        Cat cat = new Cat("Persian", "white", 4);
        System.out.println(cat.occupation(DaysOfWeek.THU));
    }

}