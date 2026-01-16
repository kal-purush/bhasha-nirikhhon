package GenericSwapMethodInteger;

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
          //first way
//          T firstElement = listForSwap.get(indexElement1);
//          T secondElement = listForSwap.get(indexElement2);
//
//          listForSwap.set(indexElement1, secondElement);
//          listForSwap.set(indexElement2, firstElement);

        //second way
//        T firstElement = null;
//        T secondElement = null;
//        int indexFirstElement = 0;
//        int indexSecondElement = 0;
//
//        for (int i = 0; i < listForSwap.size(); i++) {
//
//
//            if(i == indexElement1){
//                firstElement = listForSwap.get(i);
//                indexElement1 = i;
//            }
//
//            if(i == indexElement2){
//                secondElement = listForSwap.get(i);
//                indexElement2 = i;
//                listForSwap.remove(indexElement1);
//                listForSwap.add(indexElement1, secondElement);
//                listForSwap.remove(indexElement2);
//                listForSwap.add(indexElement2, firstElement);
//            }
//
//        }


        //third way
        T firstElement = listForSwap.get(indexElement1);
        T secondElement = listForSwap.get(indexElement2);

        listForSwap.remove(indexElement1);
        listForSwap.add(indexElement1, secondElement);
        listForSwap.remove(indexElement2);
        listForSwap.add(indexElement2, firstElement);



        for (T t : listForSwap) {
            System.out.println(t.getClass().getName() + ": " + t);
        }

    }


}