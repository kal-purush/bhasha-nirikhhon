public class Main {
    public static void main(String[] args) {
        float TicketPriceRub = 8595.15F;
        int PriceInKops = (int) (TicketPriceRub * 100); //Итоговая стоимость билета в копейках
        float OneMileCostRub =  20F;
        int OneMileCostKops = (int) (OneMileCostRub * 100); // Стоимость одной бонусной мили в копейках
        int MilesAmount =  PriceInKops / OneMileCostKops;
        System.out.println(MilesAmount);
    }
}