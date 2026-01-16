using System;


public class Program
{
    public static void Main(string[] args)
    {
        Console.WriteLine("Type out your percentage. (Do not use any characters other than numbers)");
        float score = float.Parse(Console.ReadLine());
        // I love using only if else statements <3
        if (score >= 100){
            Console.WriteLine("You got " + score + "% and your grade is: A+");
        }
        else if (score >= 90 && score <= 99) {
            Console.WriteLine("You got " + score + "% and your grade is: A");
        }
        else if (score >= 80 && score <= 89) {
            Console.WriteLine("You got " + score + "% and your grade is: B");
        }  
        else if (score >= 70 && score <= 79) {
            Console.WriteLine("You got " + score + "% and your grade is: C. You got this, keep working!");
        }
        else if (score >= 60 && score <= 69) {
            Console.WriteLine("You got " + score + "% and your grade is: D. Keep trying!");
        }
        else if (score >= 0 && score <= 59) {
            Console.WriteLine("You got " + score + "% and your grade is: E. Don't give up!");
        }
    }
}