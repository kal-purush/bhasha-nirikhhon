package aula12;

public class Exeercicio1 {
    public static void main(String[] args) {
        Integer umNumero = null;

        try {
            int resultado = umNumero + 5;
        }catch (NullPointerException e){
            String msg = e.getMessage();
            System.out.println(msg);
            e.printStackTrace();
        }

    }
}