package GenericSwapMethodStrings;

import java.util.List;

/**
 * Created by r3v3nan7 on 18.03.17.
 */
public class Box<T> {
    private String someText;


    public Box(String str){
        this.someText = str;
    }

    @Override
    public String toString(){
        return "" +this.someText.getClass().getName()+ ": " + this.someText;
    }


    public static <T> void swap(List<T> listForSwap, int indexElement1, int indexElement2){
          T firstElement = listForSwap.get(indexElement1);
          T secondElement = listForSwap.get(indexElement2);

          listForSwap.set(indexElement1, secondElement);
          listForSwap.set(indexElement2, firstElement);


        for (T t : listForSwap) {
            System.out.println(t.getClass().getName() + ": " + t);
        }

    }


}