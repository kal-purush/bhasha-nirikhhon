public class VadSkrivsUt {
    public static void changeValues(int n,
                                    CounterModel c1,
                                    CounterModel c2) {
        n++;
        c1.increment();
        c2 = new CounterModel();
        c2.increment();
    }
    public static void main(String[] args) {
        int k=5;
        CounterModel a = new CounterModel();
        CounterModel b = new CounterModel();
        changeValues(k,a,b);
        System.out.println("k="+k);
        System.out.println("a="+a.getValue());
        System.out.println("b="+b.getValue());
    } // end main
} // end VadSkrivsUt