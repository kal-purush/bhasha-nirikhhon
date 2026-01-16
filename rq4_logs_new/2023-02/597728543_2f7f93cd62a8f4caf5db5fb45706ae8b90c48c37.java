import java.util.ArrayList;

public class Goods {

        public String productName;
        public Double productPrice;

    Goods(String productName, Double productPrice) {
        this.productName = productName;
        this.productPrice = productPrice;
    }

     public static void calculate(ArrayList<Goods> goods, int quantity){
        double sum = 0;
         for (int i = 0; i < goods.size(); i++){
             System.out.println(String.format("Товар %d: %s цена: %.2f", i + 1, goods.get(i).productName, goods.get(i).productPrice));
             sum += goods.get(i).productPrice;
         }

        Double sumPerPeople = sum / quantity;
        String textRub = takeEnding(sumPerPeople.intValue());
        System.out.println(String.format("Итого с каждого человека по %.2f %s.", sumPerPeople,  textRub));
    }

    private static String takeEnding (int sumPerPeopleInt){
        int remainderOne = sumPerPeopleInt % 10;
        int remainderTwo = sumPerPeopleInt % 100;

        if (remainderTwo >= 11 && remainderTwo <= 14)
            return "рублей";
        else if (remainderOne == 1)
            return "рубль";
        else if (remainderOne > 1 && remainderOne <=4)
            return "рубля";
        else
            return "рублей";
    }
}