public class StringMetodos {
    public static void main(String[] args) {
        String nombre = "Lautaro";

        System.out.println("Tamaño string = " + nombre.length());
        System.out.println("nombre.toLowerCase() = " + nombre.toLowerCase());
        System.out.println("nombre.toUpperCase() = " + nombre.toUpperCase());
        System.out.println("nombre.equals(\"Lautaro\") = " + nombre.equals("Lautaro"));
        System.out.println("nombre.equals(\"lautaro\") = " + nombre.equals("lautaro"));
        System.out.println("nombre.equalsIgnoreCase(\"lautaro\") = " + nombre.equalsIgnoreCase("lautaro"));
        System.out.println("nombre.compareTo(\"Lautaro\") = " + nombre.compareTo("Lautaro"));
        System.out.println("nombre.compareTo(\"lautaro\") = " + nombre.compareTo("lautaro"));
        System.out.println("nombre.charAt(0) = " + nombre.charAt(0));
        System.out.println("nombre.charAt(nombre.length()-1) = " + nombre.charAt(nombre.length() - 1));
        System.out.println("nombre.substring(3,5) = " + nombre.substring(3,5));
        System.out.println("nombre.replace(\"a\", \"e\") = " + nombre.replace("a", "e"));
        
        String trabalenguas = "trabalenguas";
        System.out.println("trabalenguas.replace(\"a\", \" \") = " + trabalenguas.replace("a", ""));
        System.out.println("trabalenguas.indexOf(\"a\") = " + trabalenguas.indexOf("a"));
        System.out.println("trabalenguas.lastIndexOf(\"a\") = " + trabalenguas.lastIndexOf("a"));
        System.out.println("trabalenguas.indexOf(\"z\") = " + trabalenguas.indexOf("z"));
        System.out.println("trabalenguas.contains(\"a\") = " + trabalenguas.contains("a"));
        System.out.println("trabalenguas.indexOf(\"z\") = " + trabalenguas.indexOf("lenguas"));
        System.out.println("trabalenguas.contains(\"a\") = " + trabalenguas.contains("lenguas"));
        System.out.println("trabalenguas.startsWith(\"lenguas\") = " + trabalenguas.startsWith("lenguas"));
        System.out.println("trabalenguas.endsWith(\"lenguas\") = " + trabalenguas.endsWith("lenguas"));
        System.out.println("trabalenguas = " + trabalenguas);

    }
}