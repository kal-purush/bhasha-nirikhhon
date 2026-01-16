using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Employee : MonoBehaviour
{
    [SerializeField] private StressController stressController;
    [SerializeField] private float stressThreshold = 100;
    [SerializeField] private EmployeeState state;

    // Update is called once per frame
    void Update()
    {
        float stressLevel = stressController.GetStressLevel();
        if (stressLevel >= stressThreshold)
        {
            state = EmployeeState.OverStressed;
        }
        // show different visual states based on stress level
    }

    public float GetPercentageStress()
    {
        return stressController.GetStressLevel() / stressThreshold;
    }
}