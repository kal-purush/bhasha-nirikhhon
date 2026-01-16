package Immutable_Generics_51_52_d;

import java.util.*;

public final class Car implements Comparable<Car>{
    private final String model;
    private final List<Integer>year;
    private final CustomMutableClass mutableClass;
    private final List<CustomMutableClass> mutableList;

    public Car(String model, List<Integer>year , CustomMutableClass mutableClass, List<CustomMutableClass> mutableClasses){
        this.model = model;
        this.year = year;
        this.mutableClass =new CustomMutableClass(mutableClass) ;
    mutableList = Collections.unmodifiableList(mutableClasses);

    }
    @Override
    public  String toString() {
        return "Car{" + model + ", "+ year+ "," + mutableClass  +'}';
    }

   public   String getModel() {
       return model;
    }

    public List<Integer>getYear() {
        return year;
    }

public CustomMutableClass getCustomMutableClass(){
    return new CustomMutableClass(mutableClass);
}

public List<CustomMutableClass> getCustomMutableList(){
    return mutableList;
}

@Override
public  int compareTo(Car o) {
    return Comparator.comparing(Car::getModel)
            .thenComparing((Comparator<? super Car>) year)
            .compare(this, o);
}

    @Override
    public  boolean equals(Object o) {
        if(this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Car car = (Car) o;
        return  year == car.year && Objects.equals(model, car.model) && Objects.equals(mutableClass, car.mutableClass);
    }

    @Override
    public  int hashCode() {
        return Objects.hash(model, year, mutableClass);
    }
}