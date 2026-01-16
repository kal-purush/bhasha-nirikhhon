import java.io.*;
import java.util.*;

public class CD extends Medium{
    public CD(String title, String artist, int number, double length, boolean mark, String comment){
        super(title, artist, number, length, mark, comment);
    }

    @Override
    public String toString(){
        return "CD: " + "Anzahl der Songs = " + GetNumber() + ", " + super.toString();
    }
}