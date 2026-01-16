public abstract class WR implements IUmrechnen {

    protected String variante;
    protected String calcName;
    protected double betrag;

    public static String EUR2DOL = "Eur2Dol";
    public static String EUR2YEN = "Eur2Yen";



    private WR nextWR;

    public WR getNextWR() {
        return nextWR;
    }

    public void setNextWR(WR nextWR){
        this.nextWR = nextWR;
    }



    public String getCalcName() {
        return this.calcName;
    }

    public void setCalcName(String calcName) {
        this.calcName = calcName;
    }


    public double umrechnen(String variante, double betrag) {
        double newbetrag=0;
        System.out.println(this.calcName);
        if (this.calcName.equals(variante)) {
            System.out.println("Betrag: "+this.betrag);
            //newbetrag = umrechnen(variante, betrag);
        } else {
            if (this.nextWR != null) {
                System.out.println("Weiterleiten zum naechsten Rechner: " + nextWR.getCalcName());
                System.out.println("Betrag: "+this.betrag);
                //newbetrag = nextWR.umrechnen(variante, betrag);
            } else {
                System.out.println("Diese Waehrung kann nicht umgerechnet werden!");
                //newbetrag = 0;
            }

        }
        return newbetrag;
    }


}