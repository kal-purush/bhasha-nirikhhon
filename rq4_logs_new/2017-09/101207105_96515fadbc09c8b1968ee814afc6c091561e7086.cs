using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class elseStatement : MonoBehaviour {
	public int a, b, c;
    public Text input;
    public string hello = "Hello";
    public bool abc = false;
    public double deciOne, deciTwo;

    public string password = "password";

// Else statements are used at the end of if statements to give it a function when the if statement is FALSE and not true. 

	void Start(){
	//ONE
        if(a + b == c){
            print("Good job!");
        }else{
            print("Try again."); // if the if statement fails/is false, it will print this in all over scenarios
        }

    //TWO
        if(input.text == hello){
            print("Hello to you too!");
        }else{
            print("What?!"); // prints this if the user doesn't type "Hello" exactly. It is case sensitive.
        }

    //THREE
        if(abc){
            print("The bool is true.");
        }else{
            print("The bool is false"); // it will print this every time instead unless otherwise stated, as bools are false by default
        }

    //FOUR
        if(deciOne / deciTwo == 2){
            print("The variable deciTwo goes into deciOne twice!");
        }else{
            print("These numbers do not work. Try again."); // Will print this in all instances where the two divided doesn't equal two.
        }

    //FIVE
        if(input.text == password){
            print("The password is correct!");
        }else{
            print("The password is incorrect. Try again."); // Will print this when anything but "password" is typed in by the user.
        }


	}

}