//Written By Yitzchak Weiser

//06/07/05
import BreezySwing.*;
import javax.swing.*;
import java.util.ArrayList;
import java.awt.*;

public class Hangman extends GBFrame{
    //The parameters for all of these is (string,row,column,width,length)
    private JButton newGame=addButton("New Game",1,1,1,1);
    //Declares a button to be clicked
    private JButton click = addButton("Click Me",1,2,1,1);
    //Declares a label for directions
    private JLabel enterLetter = addLabel("Enter a Letter " ,2 ,1,1,1);
    //Declares a label for guesses left
    private JLabel guess = addLabel("Guesses Remaining",3,1,1,1);
    //Declares a text field for a letter to be inputed
    private JTextField theLetter = addTextField("",2,2,1,1);
    //Declares an integer field to output guesses remaining starts at 6 guesses
    private IntegerField guessRemaining = addIntegerField(6,3,2,1,1);
    //Declares a text field to output the word as it's guessed
    private JTextField Word = addTextField("",5,2,1,1);
    //Declares a label for directions
    private JLabel word = addLabel("Guess this Word:",5,1,1,1);
    //Declares a text field to output all of the letters that were used
    private JTextField lettersUsed = addTextField("",6,2,1,1);
    private JLabel version=addLabel("Version 3.12", 6, 1,1,1);
    //Declares a string to save the letter that was entered
    private String theletter;
    //Declares a string to save the word that needs to be guessed
    private String theWord;
    //Declares a string to save the visible word
    private String theVisibleWord;
    //Declares a word list to pick a random word
    private WordList list;
    //Instantiates array list to save all used letters
    private ArrayList usedLetters = new ArrayList();
    private JTextArea theMan;
    //The constructor
    public Hangman(){
        setTitle("Hangman");
        
        //disables the integer field from editing
        guessRemaining.setEnabled(false);
        //disables editing of the visible word
        Word.setEditable(false);
        //disables editing of the used letters array
        theMan=addTextArea("                            \n        ",1,3,1,6);
        theMan.setEditable(false);
        lettersUsed.setEditable(false);
        newGame();
    }
    public void newGame(){
        //Instantiates the word list
        list = new WordList();
        //gets a random word from word list and sets it to theWord
        theWord= list.getword().toUpperCase();
        //this will output the answer--use only if debugging
       //  System.out.println(theWord);
        //clears the visible word
        theVisibleWord="";
        //sets the visible word to all underscores
        setVisibleWord();
        //displays the underscores
        Word.setText(theVisibleWord);
        usedLetters.clear();
        guessRemaining.setNumber(6);
        theMan.setText("");
        click.setEnabled(true);
        theLetter.setEnabled(true);
        lettersUsed.setText("");
        theLetter.setText("");
        //brings cursor back to the jtextfield
        theLetter.requestFocus();
    }
    
    public void buttonClicked(JButton buttonObj){
        if (buttonObj==click){
            //clears the letter
            theletter=null;
            //sets a boolean to false to determine if a change was made
            boolean changed=false;
            //saves the inputted letter to theLetter, only saves the first letter
            theletter =theLetter.getText().substring(0,1).toUpperCase();
            if (theletter.equals(",")|| theletter.equals(".") || theletter.equals(";")
            || theletter.equals("/") || theletter.equals("[") || theletter.equals("]") || theletter.equals(" ")){
                theLetter.setText("");
                theLetter.requestFocus();
                messageBox("Envalid Entry.");
                return;
            }
            //otherwise checks if the letter is already used
            if (checkArray()){
                //compares theletter to every letter in theWord
                //by increasing the substring each time by one
                //substring (begin index, end index) not inclusive
                for (int i = 0; i< theWord.length(); i++){
                    if (theletter.equalsIgnoreCase(theWord.substring(i, (i + 1) ) ) ){
                        change(theletter, i);
                        changed=true;
                    }
                }
                //calls the method youWin and if true outputs string in a message box
                if (youWin()) { messageBox("You Win!!!");
                //disables future button clicking
                click.setEnabled(false);
                }
                //if nothing was changed decrease the number of guesses remaining
                if (!changed) {guessRemaining.setNumber(guessRemaining.getNumber()-1);
                showMan();
                }
                //if no more guesses remaining calls youLoose()
                if (guessRemaining.getNumber()==0)
                    youLoose();
                
            }
            //if a letter was already used once output this string
            else{
                messageBox("Enter a New Letter, Letter Already Used.");
            }
            //clear theLetter
            theLetter.setText("");
            //brings cursor back to the jtextfield
            theLetter.requestFocus();
        } else newGame();
    }
    
    public void  change(String let, int pos){
        //declares two temporary strings and a new output string
        String temp1="", temp2="", newoutput;
        // declares two booleans to tell if temp1 or temp2 is null
        boolean tem1 =false,tem2= false;
        //if the letter found is not the first letter in the visible word
        if (pos>0){
            //saves the part of the visible string from the beginning until the position but not including position
            temp1 = theVisibleWord.substring(0,pos);
            tem1=true;  }
        //if the letter found is not the last letter in the visible word
        if (pos < theWord.length()) {
            //saves the part of the visible string from the letter after position until the end
            temp2 = theVisibleWord.substring(pos+1);
            tem2=true;
        }
        //if temp2 is null
        if(!tem2)
            //cancantanate the first part of the word to theletter
            newoutput = temp1 + let;
        //if temp1 is null
        if(!tem1)
            //cancantanate the last part of the word to theletter
            newoutput =let+ temp2;
        //otherwise cancantanate beginning to letter to the end
        else newoutput = temp1 + let + temp2;
        theVisibleWord=newoutput;
        //output the new visible word
        Word.setText(newoutput);
    }
    public void youLoose(){
        //if you lose display message box and disable future button click
        messageBox("You Lost! The Word Was "+theWord+".");
        click.setEnabled(false);
        theLetter.setEnabled(false);
    }
    public void showMan(){
        if (guessRemaining.getNumber()==5)
            theMan.setText("\t------ \n\t |  | \n\t |  () \n\t |   \n\t | \n\t | \n\t===");
        if (guessRemaining.getNumber()==4)
            theMan.setText("\t------ \n\t |  | \n\t |  () \n\t |  /\\ \n\t | \n\t | \n\t===");
        if (guessRemaining.getNumber()==3)
            theMan.setText("\t---------- \n\t |        |\n\t |       ()\n\t |      -/\\ \n\t | \n\t | \n\t===");
        if (guessRemaining.getNumber()==2)
            theMan.setText("\t---------- \n\t |       | \n\t |       () \n\t |      -/\\- \n\t | \n\t | \n\t===");
        if (guessRemaining.getNumber()==1)
            theMan.setText("\t---------- \n\t |       | \n\t |       () \n\t |      -/\\- \n\t |       | \n\t | \n\t===");
        if (guessRemaining.getNumber()==0){
            theMan.setText("---------- \n |       | \n |       () \n |      -/\\- \n |       || \n | \n===\n\nYOU'VE BEEN\n    HANGED!!!");
            Font h= new Font("Arial", Font.PLAIN, 18);
            theMan.setFont(h);
        }
    }
    public boolean youWin(){
        //checks if you win by comparing every letter in the visible word to the underscore
        //if you find the underscore then you did not win yet and returns false
        for (int i = 0; i <theWord.length(); i++){
            if (theVisibleWord.substring(i,i+1).equals("_"))
                return false;
        }
        return true;
    }
    public boolean checkArray(){
        //checks an array list if you used the letter already
        //if it finds the letter then returns false
        int i;
        for (i = 0; i <usedLetters.size(); i++){
            if ( ( (String) usedLetters.get(i)).equalsIgnoreCase(theletter))
                return false;
        }
        //otherwise adds the letter to the array list
        usedLetters.add(theletter);
        //displays the array list by calling the toString method and outputting it to jtextfield
        lettersUsed.setText(usedLetters.toString().substring(1,usedLetters.toString().length()-1 ));
        
        return true;
    }
    public void setVisibleWord(){
        //sets the visible word to as many underscores as theWord at the beginning of the game
        for (int i=0; i<theWord.length(); i++){
            theVisibleWord=theVisibleWord+"_";
        }
    }
    
    public static void main(String [] args){
        Hangman theGUI=new Hangman();
        theGUI.setSize(600,250);
        theGUI.setVisible(true);
    }
}