
//Programmering 28-09-2017

public class Metoder {
    public static void main(String[] args) {
        System.out.println(1+2);
        Metoder m = new Metoder(); //Til Sum
        int resultat = m.Sum(10, 20);
        System.out.println(resultat);

        resultat = m.Sum(15, 25); //Til Sum med andet resultat
        System.out.println(resultat);

        udskrivHej();
        udskrivHej();
        udskrivHej();

        double skabnen = 6 * Math.random() + 1;
        System.out.println(skabnen);

        System.out.println(m.Sum3(8, 5, 10));

        indtastOgVisNavn("Michael");
    }
    public Metoder(){

    }
    public int Sum(int a, int b){
        int res = a + b;
        return res;
    }
    public static void udskrivHej(){
        System.out.println("Hej");
    }

    public int Sum3(int a, int b, int c){
        return a + b +c;

    }

    public static void indtastOgVisNavn(String navn){
        System.out.println("Hej " + navn);
    }
}