package GeekBrians.Slava_5655380.Homework.Lesson7.FirstHalf;

import java.util.function.Function;

// ЗАДАНИЕ 1. Создайте три класса Человек, Кот, Робот, которые не наследуются от одного класса.
// Эти классы должны уметь бегать и прыгать (методы просто выводят информацию о действии в консоль).
public class Cat implements Сompetitor {
    private int manaAmount;

    private boolean act(Function<Integer, Boolean> actFunc, int waste) {
        System.out.print("Кот ");
        // ЗАДАНИЕ 4. У препятствий есть длина (для дорожки) или высота (для стены), а участников ограничения на бег и прыжки.
        // Если участник не смог пройти одно из препятствий, то дальше по списку он препятствий не идет.
        if (waste <= manaAmount) {
            actFunc.apply(waste);
            manaAmount -= waste;
            return true;
        }
        actFunc.apply(manaAmount);
        manaAmount = 0;
        return false;
    }

    public Cat(int manaAmount) {
        this.manaAmount = manaAmount;
    }

    @Override
    public boolean run(int length) {
        // Код переписанных методов run и jump вынесен в отдельную функцию просто чтобы показать что в этом классе он одинаковый
        return act(Сompetitor.super::run, length);
    }

    @Override
    public boolean jump(int height) {
        // Код переписанных методов run и jump вынесен в отдельную функцию просто чтобы показать что в этом классе он одинаковый
        return act(Сompetitor.super::jump, height);
    }
}