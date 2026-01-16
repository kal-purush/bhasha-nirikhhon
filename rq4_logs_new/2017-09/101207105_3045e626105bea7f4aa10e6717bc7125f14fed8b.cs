using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class mathOperatorsTwo : MonoBehaviour {

    public float num1, num2, addResult, subResult, multResult, divResult, remainderResult;
    

    void Update(){
        addResult = num1 + num2;
        subResult = num1 - num2;
        multResult = num1 * num2;
        divResult = num1 / num2;

        if(num1 != 0 && num2 != 0){
            divResult = num1 / num2;
        }else{
            print("Can't divide by 0.");
        }
        remainderResult = num1 % num2;

    }

}