public class GenericRawAssignment {

    public static void main(String[] args) {
        /**
         * Code block below works with generic objects being assigned
         * to a reference variable of a raw type - no type information provided
         */
        
        Movable movable = new Movable<Car>();   // This line compiles without error or warning.
        movable.set(new Car());                 // Compiles with unchecked warning - raw type loses type information of movable so compiler can check for type safety
        //Car car = movable.get();                // Compiler error - type information not set on Movable so T is of type Object. Cannot be assigned without explicit cast
        Car car = (Car) movable.get();          // This line replaces one above to ensure successful compilation.
        
        /**
         * Code block below works with raw objects being assigned
         * to a reference variable of a generic type - type information provided
         */
        
        Movable<Car> anotherMovable = new Movable();    // Unchecked conversion warning
        anotherMovable.set(new Car());      // No warnings here We have type information for anotherMovable so the compiler is able to verify the object we are passing as argument to the method
        Car anotherCar = movable.get();     // No need for explicit cast here - we have type information for movable so will know what is returned from get() is of type Car so can be assigned to variable of type Car
    
    
    }
}

class Movable<T> {

    private T t;
    public void set(T t) {
        this.t = t;
    }
    
    public T get() {
        return t;
    }

}

class Car {
    
}