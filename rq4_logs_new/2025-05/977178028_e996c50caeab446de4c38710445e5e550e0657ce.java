package um.programacion2.libro;

import um.programacion2.exception.IllegalArgumentsLibro;

public class LibroFactory {
    private static Long idCounter = 0L;

    public static Libro createLibro(String isbn, String titulo, String autor) {
        verifyIsbn(isbn);
        verifyTitulo(titulo);
        verifyAutor(autor);
        EstadoLibro estado = EstadoLibro.DISPONIBLE;
        return new Libro(++idCounter, isbn, titulo, autor, estado);
    }

    public static void verifyIsbn(String ISBN) throws IllegalArgumentsLibro {
        // Verificar si el ISBN es válido
        if (ISBN == null || ISBN.trim().isEmpty()) {
            throw new IllegalArgumentsLibro("No puede ser nulo o vacío");
        }

        String isbnLimpio = getIsbnLimpio(ISBN);

        if (!validarDigitoControl(isbnLimpio)) {
            throw new IllegalArgumentsLibro("Codigo Erroneo (no coincide con el digito de control)");
        }
    }

    public static String getIsbnLimpio(String ISBN) throws IllegalArgumentsLibro {
        String isbnLimpio = ISBN.replaceAll("[-\\s]", "");

        if (isbnLimpio.length() != 10 && isbnLimpio.length() != 13) {
            throw new IllegalArgumentsLibro("No tiene 10 ni 13 dígitos");
        }

        // Verificar que solo contenga dígitos (y posiblemente 'X' en la última posición para ISBN-10)
        if (!isbnLimpio.matches("[0-9]{9}[0-9X]") && !isbnLimpio.matches("[0-9]{13}")) {
            throw new IllegalArgumentsLibro("Contiene caracteres inválidos");
        }
        return isbnLimpio;
    }

    public static boolean validarDigitoControl(String isbnLimpio) {
        int suma = 0;
        if (isbnLimpio.length() == 10) {
            // Validación para ISBN-10
            for (int i = 0; i < 9; i++) {
                suma += (isbnLimpio.charAt(i) - '0') * (10 - i);
            }

            char ultimoCaracter = isbnLimpio.charAt(9);
            int digitoControl = (ultimoCaracter == 'X') ? 10 : (ultimoCaracter - '0');

            return (suma + digitoControl) % 11 == 0;
        } else {
            // Validación para ISBN-13
            for (int i = 0; i < 12; i++) {
                suma += (isbnLimpio.charAt(i) - '0') * (i % 2 == 0 ? 1 : 3);
            }

            int digitoControl = isbnLimpio.charAt(12) - '0';
            int digitoCalculado = (10 - (suma % 10)) % 10;

            return digitoControl == digitoCalculado;
        }
    }

    public static void verifyTitulo(String titulo) {
        // Verificar si el título es válido
        if (titulo == null || titulo.trim().isEmpty()) {
            throw new IllegalArgumentsLibro("El título no puede ser nulo o vacío");
        }
    }

    public static void verifyAutor(String autor) {
        // Verificar si el autor es válido
        if (autor == null || autor.trim().isEmpty()) {
            throw new IllegalArgumentsLibro("El autor no puede ser nulo o vacío");
        }
    }
}