public class StringArreglos {
    public static void main(String[] args) {
        String trabalenguas = "trabalenguas";
        System.out.println("trabalenguas.toCharArray() = " + trabalenguas.toCharArray());
        char[] arreglo= trabalenguas.toCharArray();
        for(int i = 0; i< arreglo.length; i++){
            System.out.println("arreglo[i] = " + arreglo[i]);
        }
        System.out.println("trabalenguas = " + trabalenguas.split("a"));
        String[] arreglo1 = trabalenguas.split("a");
        for (int j = 0; j < arreglo1.length; j++) {
            System.out.println("arreglo1 = " + arreglo1[j]);
        }

        String archivo = "alguna_imagen.pdf";
        String[] archivoArr = archivo.split("\\.");
        System.out.println("archivoArr[archivoArr.length-1] = " + archivoArr[archivoArr.length-1]);
    }
}