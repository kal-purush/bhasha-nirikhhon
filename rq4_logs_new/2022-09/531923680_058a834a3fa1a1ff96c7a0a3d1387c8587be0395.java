public class EjemploStringInmutable {
    public static void main(String[] args) {
        String curso = "Programación Java";
        String profesor = "Ni idea quien es";

        curso.concat(profesor);
        String resultado = curso.concat(profesor);
        System.out.println(curso);
        System.out.println(resultado);

        String resultado2 = curso.transform(c -> { return c + " con " + profesor;});
        System.out.println("curso = " + curso);
        System.out.println("resultado2 = " + resultado2);
        resultado.replace("a", "A");
        String resultado3 = resultado.replace("a", "A");
        System.out.println("resultado = " + resultado);
        System.out.println("resultado3 = " + resultado3);






    }
}