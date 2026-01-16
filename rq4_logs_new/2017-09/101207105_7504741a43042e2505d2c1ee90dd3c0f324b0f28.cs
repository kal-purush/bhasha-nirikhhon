using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class arrays : MonoBehaviour {

    public int[] myArrayOne = new int[5]; // example one, states array contains five separate integers.
    public int[] myArrayTwo = {16, 21, 26, 31, 36}; /* example two, same as the array above, this initiates an array of five integers, 
                                                       right off the bat as opposed to later in the start function. */
    public string[] stringArray = new string[4]; // example three, initializes a string array.

    public string[] stringArrayTwo = {"Bob", "Dillon", "Sam", "Marley",}; // example four, einitializes the same string array as above, just initialized sooner.

    public double[] doubleArray = new double[3]; // example five, initializes a double array with 3 "spots"

    public double[] doubleArrayTwo = {0.2, 0.3, 0.4}; // example six, initializes the same double array above, just upfront.

    public float[] floatArray = new float[2]; // example seven, initializes a float array with two "spots"

    public float[] floatArrayTwo = {.5f, .6f}; // example eight, intitializes the same float array as above just upfront.

    public bool[] boolArray = new bool [2]; // example nine, initializes a bool array.

    public bool[] boolArrayTwo = {true, false}; // example ten, initializes a bool array with two slots.


    void start(){
        myArrayOne[0] = 16; // says array #0 contains 16.
        myArrayOne[1] = 21; // assigns 21 to #1 or the second space in the array.
        myArrayOne[2] = 26; // assigns 26 to #2 or the third space in the array.
        myArrayOne[3] = 31; // assigns 31 to #3 or the fourth space in the array.
        myArrayOne[4] = 36; // assigns 36 to #4 or the fifth space in the array.
        // Although the array ends on [4], there are five separate integers stated as Arrays begin with the number zero.

        stringArray[0] = "Bob"; // assigns Bob to the first spot in the array.
        stringArray[1] = "Dillon"; // assigns Dillon to the second spot.
        stringArray[2] = "Sam"; // assigns Sam to the third spot.
        stringArray[3] = "Marley"; // assigns Marley to the fourth spot, ending the array.

        doubleArray[0] = 0.2; // assigns .2 to the first spot in the array.
        doubleArray[1] = 0.3; // assigns .3 to the second spot.
        doubleArray[2] = 0.4; // assigns .4 to the third spot.

        floatArray[0] = 0.5f; // assigns .5 to the first spot, "f" is required with float variables.
        floatArray[1] = 0.6f; // assigns .6 to the second spot, "f" again is required.

        boolArray[0] = true; // assigns true to the first spot in the array.
        boolArray[1] = false;  // assigns false to the seccond spot in the array.

    // prints the outcomes of the arrays already assigned values above.
        print(myArrayTwo);
        print(stringArrayTwo);
        print(doubleArrayTwo);
        print(floatArrayTwo);
        print(boolArrayTwo);


    
    }

}