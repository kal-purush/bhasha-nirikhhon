package Animals;

import java.util.Objects;

public class Amphibians extends Animals{

    private  final String livingEnvironment;

    public Amphibians(String name, int yearsOld, String livingEnvironment) {
        super(name, yearsOld);
        this.livingEnvironment = livingEnvironment;
    }

    public String hunts() { // охотиться
        return "Охочусь на движущие цели";
    }

    @Override
    public String eat() { // кушать
        return "Схватываю добычу челюстями, съедаю живьем";
    }

    @Override
    public String sleep() { // спать
        return " сплю под землей, разгребая мягкий грунт своими задними лапками";
    }

    @Override
    public String move() { // перемещаться
        return "Прыгаю, ползаю, плаваю";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        Amphibians that = (Amphibians) o;
        return Objects.equals(livingEnvironment, that.livingEnvironment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), livingEnvironment);
    }

    @Override
    public String toString() {
        return super.toString()+ "livingEnvironment = " + livingEnvironment +  ".";
    }

    public String getLivingEnvironment() {
        return livingEnvironment;
    }
}