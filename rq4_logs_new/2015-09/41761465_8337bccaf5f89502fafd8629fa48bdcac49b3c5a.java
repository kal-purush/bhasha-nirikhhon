package Google;


/**
 * Write a description of class test here.
 * 
 * @author (your name) 
 * @version (a version number or a date)
 */
public class test
{
    public static void main(String[] args)
    {
        SpecialString string_1 = new SpecialString("1011111111111100000");
        System.out.println(string_1.stringSwitch());
        System.out.println(string_1.stringReverse());
        SpecialString string_2 = SpecialString.infiniteSequence(4);
        System.out.println(string_2);
        
    }
}