using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Condition : MonoBehaviour
{
    public bool fullHealth;
    public bool lowHealth;

    public bool CheckConditions(Unit _unit) {
        bool result = true;
        // Health Conditions
        if(fullHealth) {
            if (!ConditionController.HealthIsFull(_unit)) {
                result = false;
            }
        }
        if(lowHealth) {
            if (!ConditionController.HealthIsLow(_unit)) {
                result = false;
            }
        }

        return result;
    }
}