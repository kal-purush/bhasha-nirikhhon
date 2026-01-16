using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Syntax : MonoBehaviour { // Here "Syntax" is a sub class of "MonoBehaviour". It will inherit all of MonoBehaviour's functions.

    // You want to make sure all single lines have a semi-colon at the end to note the finish of that line of code.
    // While the entire functions are starting and ending with curly brackets.
    // For every open curly bracket, there must be one that coincides with it to close it/the function!

    int multiply, multiplyBy, addOne, addTwo, sum, product; // assigning multiple variables of type integer at once.

        void Start(){ // Indented slightly further than the code above to provide structure/organization
            
            /*  Here we will start initializing those variables above with values.
             *  Notice, I'm using multi-line comment to write this. You can do so as shown here, using the slash and asterisk
             *  I'm simply putting the additional asterisks for organizational sake, it is not necessary.
             */
            multiply = 2;
            multiplyBy = 9;
            addOne = 55;
            addTwo = 49;

            product = multiply * multiplyBy; // multiplies the two multiplication integers together, listed above.

            print("If you times " + multiply + " by " + multiplyBy + " you get " + product); // prints the end result of the multiplication.
   
            //similarly you can do the same with addition.
            sum = addOne + addTwo; // adds the two integers together, listed above.

            print("If you add " + addOne + " by " + addTwo + " you get " + sum); // prints the end result of the addition.

   
   
    }   // Closing curly bracket to the Void Start function, this closes out/stops that function.
} // Closing curly bracket to Overall class. Marks the end of this program.