public class ConversionDeTipo {
    public static void main(String[] args) {
        String numeroStr = "50";
        
        int numeroInt = Integer.parseInt(numeroStr);

        System.out.println("numeroInt = " + numeroInt);
        
        String realStr = "98251.26e-3";
        double realDouble = Double.parseDouble(realStr);
        System.out.println("realDouble = " + realDouble);
        
        String logicoStr = "true";
        boolean bool = Boolean.parseBoolean(logicoStr);
        System.out.println("bool = " + bool);

        int otroNumeroInt = 100;
        System.out.println("otroNumeroInt = " + otroNumeroInt);

        String otroNumeroStr = Integer.toString(otroNumeroInt);
        System.out.println("otroNumeroStr = " + otroNumeroStr);

        String nuevamenteNumeroInt = String.valueOf(otroNumeroInt+10);
        System.out.println("nuevamenteNumeroInt = " + nuevamenteNumeroInt);

        int i = 1000000;
        short s = (short)i;
        long l = i;

        System.out.println("l = " + l);
        System.out.println("s = " + s);

    }
}