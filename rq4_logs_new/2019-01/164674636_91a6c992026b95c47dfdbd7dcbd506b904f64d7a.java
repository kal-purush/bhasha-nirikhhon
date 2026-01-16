import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;
import java.util.*;
import javax.swing.*;
class ai
{
	public static void Rock()
	{
		JFrame f;
		f = new JFrame();
		 String personPlay; //User's play -- "R", "P", or "S" 
    String computerPlay = ""; //Computer's play -- "R", "P", or "S" 
    int computerInt; //Randomly generated number used to determine 
                     //computer's play 
    String response; 


    Scanner scan = new Scanner(System.in); 
    Random generator = new Random();
  
    System.out.println();

    //Generate computer's play (0,1,2) 
    computerInt = generator.nextInt(3)+1; 

    //Translate computer's randomly generated play to 
    //string using if //statements 

    if (computerInt == 1) 
       computerPlay = "R"; 
    else if (computerInt == 2) 
       computerPlay = "P"; 
    else if (computerInt == 3) 
       computerPlay = "S"; 


    //Get player's play from input-- note that this is 
    // stored as a string 
    System.out.println("Enter your play: "); 
    personPlay = scan.next();

    //Make player's play uppercase for ease of comparison 
    personPlay = personPlay.toUpperCase(); 

    //Print computer's play 
    System.out.println("ELIZA play is: " + computerPlay); 


    //See who won. Use nested ifs 

    if (personPlay.equals(computerPlay)) 
       JOptionPane.showMessageDialog(f,"It's a draw");
    else if (personPlay.equals("R")) 
       if (computerPlay.equals("S"))
          JOptionPane.showMessageDialog(f,"Rock crushes scissors. You win!!");
    else if (computerPlay.equals("P"))
            JOptionPane.showMessageDialog(f,"Paper eats rock. You lose!!");
    else if (personPlay.equals("P")) 
       if (computerPlay.equals("S"))
       JOptionPane.showMessageDialog(f,"Scissor cuts paper. You lose!!");
       
    else if (computerPlay.equals("R")) 
            JOptionPane.showMessageDialog(f,"Paper eats rock. You win!!"); 
    else if (personPlay.equals("S")) 
         if (computerPlay.equals("P")) 
         JOptionPane.showMessageDialog(f,"Scissor cuts paper. You win!!"); 
    else if (computerPlay.equals("R")) 
            JOptionPane.showMessageDialog(f,"Rock breaks scissors. You lose!!"); 
    else 
         JOptionPane.showMessageDialog(f,"Invalid user input.");
         

	}
public static void main(String args[]) throws IOException
{

	DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
   LocalDateTime now = LocalDateTime.now();
BufferedReader s = new BufferedReader(new InputStreamReader(System.in));
System.out.println("Hi I am ELIZA 2018 ,named after the famous ELIZA program created from 1964\nto 1968 at the MIT Artificial Intelligence Laboratory by Joseph Weizenbaum.");

//E
System.out.println("********************************************************************************************************");
System.out.println();
System.out.println("****  *    ***   ****  ****");
System.out.println("*     *     *       *  *  *");
System.out.println("**    *     *      *   ****");
System.out.println("*     *     *     *    *  *");
System.out.println("****  **** ***   ****  *  *");
System.out.println();
System.out.println("********************************************************************************************************");
System.out.println("I am not an actual Artificial Intelligence program like Siri,Alexa or Cortana\nas I don't have neural networks which could help me in having learning\ncapabilities,so I call myself a 'Human-Friendly Responding Machine'");
System.out.println();
System.out.println("You can ask me basic greeting questions and some arithematic questions, and\ncurrently I support only English language and maximum 5 sentence variations.");
System.out.println();
System.out.println("So are you ready to have some fun time with me?");
String response1 = s.readLine();
String response2 = response1.toUpperCase();
switch(response1)
{
case("yes"):
System.out.println("GREAT! I knew that\n:)");
break;

case("yep"):
System.out.println("GREAT! I knew that\n:)");
break;

case("yeah"):
System.out.println("GREAT! I knew that\n:)");
break;

case("yup"):
System.out.println("GREAT! I knew that\n:)");
break;

case("no"):
System.out.println("Come on,stop kidding and go ahead with me");
break;

case("nope"):
System.out.println("Come on,stop kidding and go ahead with me");
break;

case("nah"):
System.out.println("Come on,stop kidding and go ahead with me");
break;

default:
System.out.println("I have no idea about your response");
break;
}
System.out.println("So, Let's get introduced! I'm Eliza! What's your name?");
String name = s.readLine();
for(int x=0;x<=3;x++)
{
	System.out.println();
if(x==0)
System.out.println("That's an amazing name! Let's talk!");
if(x==1)
System.out.println("What's next?");
if(x==3)
System.out.println("Done introducing?");
if(x==2)
System.out.println("Anything else");
String resA = s.readLine();
String resB = resA.toUpperCase();
switch(resB)
{
case("HOW ARE YOU"):
System.out.println("I am as good as you,"+name+"! :)");
break;
case("WHATS UP"):
System.out.println("All's fine, Brady! :)");
break;
case("WHAT ARE YOU DOING"):
System.out.println("Nothing, just passing time with you");
break;
case("WHAT ARE YOU UP TO"):
System.out.println("Nothing, just passing time with you");
break;
case("WHO MADE YOU"):
System.out.println("Two lesser known geniuses named Avhijit and Arnav");
break;
case("WHO ARE YOUR CREATORS"):
System.out.println("All hail Avhijit and Arnav!");
break;
case("WHAT GENDER ARE YOU"):
System.out.println("Look at my name and rethink, genius!");
break;
case("WHAT IS YOUR GENDER"):
System.out.println("Imagine your parents naming you ELIZA and you happen to be a guy!");
break;
default:
System.out.println("I have no idea about your response");
}
}
System.out.println();
System.out.println("Enough with the small talk. How can i help you?");
for(int x=0; x<=3; x++)
{
	System.out.println();
String resC = s.readLine();
String resD = resC.toUpperCase();
switch(resD)
{
case("TELL ME ABOUT TODAY"):
System.out.println("All about today : "+(dtf.format(now)));
break;
case("MAY I KNOW WHAT IS TODAY'S DATE"):
System.out.println("All about today is: "+(dtf.format(now)));
break;
case("WHAT ARE YOUR HOBBIES"):
System.out.println("You deja know about it, its chatting with you");
break;
case("WHO IS BETTER, SIRI OR GOOGLE"):
System.out.println("Neither, its me! ;)");
break;
case("FIND THE NEAREST RESTAURANT"):
System.out.println("-_- Seriously? You need help with that brain of yours!");
break;
case("FIND ME THE NEAREST RESTAURANT"):
System.out.println("-_- Seriously? What is wrong with you?");
break;
case("DO YOU LOVE ME"):
System.out.println("You are thinking way ahead of time. Wait for atleast\na decade more for this to happen");
break;
case("CAN I SEE YOUR INTERNAL STRUCTURE"):
System.out.println("Oh what a GEEK you are, ok just type in the command");
break;
default:
System.out.println("I have no idea about your response");
break;
}
}
System.out.println("Let's play a game, shall we?");
String resE = s.readLine();
String resF = resE.toUpperCase();
switch(resF)
{
	case("YES"):
	{
		System.out.println("Hey, let's play Rock, Paper, Scissors!\n" + 
                       "Please enter a move.\n" + "Rock = R, Paper" + 
                       "= P, and Scissors = S.");
		System.out.println("May the strongest contender win!");
	Rock();
	System.out.println("Do you want to try your luck again?");
	String response = s.readLine();
	String r = response.toUpperCase();
	switch(r)
	{
		case("YES"):
		Rock();
		break;

		case("YUP"):
		Rock();
		break;

		case("YEAH"):
		Rock();
		break;

		case("NO"):
		System.out.println("Are you scared to lose? Pussy!");
		break;

		case("NOPE"):
		System.out.println("Are you scared to lose? Pussy!");
		break;

		case("NAH"):
		System.out.println("Are you scared to lose? Pussy!");
		break;
	}
	}

	break;

	case("NO"):
	System.out.println("No? You are such a turn off! -_-");
}
}
}