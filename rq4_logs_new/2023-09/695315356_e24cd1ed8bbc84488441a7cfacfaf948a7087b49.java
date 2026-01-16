package Java.programacionImperativa.organizacionCodigo.Ejercicios;

public class Ejecicio10 {
    public static void main(String[] args) {
        double acum =0;
        double n1 =2;
        double n2 =4;

        acum+= (n1*n2)/1;       //8
        acum+=(n1*n2)/2;        //4
        acum+=(n1*n2)/3;        //2.66
        acum+=(n1*n2)/4;        //2
        acum+=(n1*n2)/5;        //1.6
        acum+=(n1*n2)/6;        //1.33
        acum+=(n1*n2)/7;        //1,14
        acum+=(n1*n2)/8;        //1
        acum+=(n1*n2)/9;        //0.88  
        acum+=(n1*n2)/10;       //0.8
        acum+=(n1*n2)/11;       //0.72
        acum+=(n1*n2)/12;       //0.66
        acum+=(n1*n2)/13;       //0.61
        acum+=(n1*n2)/14;       //0.57
        acum+=(n1*n2)/15;       //0.53

        System.out.println("El resultado es: " + acum);

        //-------------------------------------------------------
        System.out.println("");

        acum =0;
        for(int i=1;i<=15;i++)
        {
            acum+=(n1*n2) / i;
        }
        System.out.println("El resultado es: " + acum);
    }
}