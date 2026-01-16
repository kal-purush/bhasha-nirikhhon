public class DecorateLogging extends Decorator {

    public DecorateLogging(WR wr) {
        super(wr);
    }

    @Override
    public double umrechnen(String variante, double betrag)
    {
        double result = this.waerungsrechner.umrechnen(variante, betrag);
        betrag = Math.round(betrag*100.0)/100.0;
        System.out.print("Umrechnugsvorgang, " + variante + " mit dem Betrag ");
        System.out.printf(  "%.2f", betrag);
        System.out.print(". \n");
        System.out.println("Betrag exklusive Umrechnungsgebühren!");
        System.out.println(" ");
        return result;
    }

}