
package adapter;

public class Car implements Vehicle{

    @Override
    public void SpeedUp() {
        System.out.println("Car Speed Up");
    }

    @Override
    public void Brake() {
        System.out.println("Car Brake");
    }
    
}