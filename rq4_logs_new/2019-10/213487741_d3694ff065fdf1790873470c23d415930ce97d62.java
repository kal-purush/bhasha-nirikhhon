import java.util.Scanner;

class Method_dieRoll {
    public static void main(String[] args)
    {
        Scanner sc = new Scanner(System.in);
        boolean agane = true;
        while (agane == true) 
        {
        System.out.println("How many dice do you want to roll?");
        int rollnum = sc.nextInt();
        System.out.println("How many sides should the dice have?");
        int sidenum = sc.nextInt();
        dieRoll(rollnum, sidenum);
        System.out.println("go agane? (y/n)");
        String answer = sc.next();
        if (answer.equals("y")) agane = true;
        else if (answer.equals("n")) agane = false;
    }
    }
    public static void dieRoll (int rolls, int sides) {
        int total = 0;
                if (rolls <= 0)
        {
            rolls = 10;
            System.out.println("nice try idiot (rolls auto-set to 10)");
        }
        if (sides <= 0)
        {
            sides = 6;
            System.out.println("really?... (dice sides auto-set to 6)");
        }
        System.out.print("Your rolls were: ");
        
        for (int i = 1; i <= rolls; i++)
        {
            int roll = (int) (Math.random()*sides) + 1;
            System.out.print(roll + " ");
            total = total + roll;
        }
        
        System.out.println("");
        System.out.println("For a total of: " + total);
    }
}