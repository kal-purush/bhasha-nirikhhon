package GeekBrians.Slava_5655380.Homework.Lesson6;

public class Bowl {
    private int food = 0;

    Bowl() {
        food = 0;
    }

    Bowl(int amount) {
        putFood(amount);
    }

    // ЗАДАНИЕ 6. Добавить в тарелку метод, с помощью которого можно было бы добавлять еду в тарелку.
    public void putFood(int amount) {
        food += amount;
        System.out.println("Food in the bowl refilled, now there's " + food + " food units.");
    }

    public int decreaseFood(int amount) {
        int lastFoodValue = food;
        // ЗАДАНИЕ 2. Сделать так, чтобы в тарелке с едой не могло получиться отрицательного количества еды
        // (например, в миске 10 еды, а кот пытается покушать 15-20).
        if (amount > this.food)
            food = 0;
        else
            food -= amount;
        System.out.println("Food in the bowl decreased, now there's " + food + " food units.");
        return lastFoodValue - food;
    }

    public int getFood() {
        return food;
    }
}