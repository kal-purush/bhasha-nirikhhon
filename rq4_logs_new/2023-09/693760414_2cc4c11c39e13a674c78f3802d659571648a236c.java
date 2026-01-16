package Karakter.app;

public class Grade {
    private String emnekode;
    private char karakter;

    public Grade(String emnekode, char karakter) {
        this.emnekode = emnekode;
        this.karakter = karakter;
    }

    public String getEmnekode() {
        return emnekode;
    }

    public char getBokstavKarakter() {
        return karakter;
    }

    public int getTallKarakter() {
        switch (karakter) {
            case 'A':
                return 5;
            case 'B':
                return 4;
            case 'C':
                return 3;
            case 'D':
                return 2;
            case 'E':
                return 1;
            case 'F':
                return 0;
            default:
                return -1;
        }
    }
}