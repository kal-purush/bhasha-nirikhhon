import org.omg.CosNaming.NamingContextExtPackage.StringNameHelper;

public class Car {


    String brand;
    Myself holder;

    Car(String brandName, Myself holderName){
        this.brand = brandName;
        this.holder = holderName;
        }

        void showHolderName(){
            System.out.println("my name is " + holder.name + " my car is " + this.brand);
        }

    
    
    
}