package xceder;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: Mac
 * Date: 2018-02-05
 * Time: 下午2:07
 */
public class Car {
    private String name;
    private int speed;
    private int wheel;

    public Car() {
    }

    public Car(String name, int speed, int wheel) {
        this.name = name;
        this.speed = speed;
        this.wheel = wheel;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getSpeed() {
        return speed;
    }

    public void setSpeed(int speed) {
        this.speed = speed;
    }

    public int getWheel() {
        return wheel;
    }

    public void setWheel(int wheel) {
        this.wheel = wheel;
    }
}