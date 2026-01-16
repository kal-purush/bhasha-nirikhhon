using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using States;

public class NewClassTypeState : State<Student>
{
    private static NewClassTypeState _instance;

    private NewClassTypeState()
    {
        if (_instance != null)
        {
            return;
        }

        _instance = this;
    }

    public static NewClassTypeState Instance
    {
        get
        {
            if (_instance == null)
            {
                new NewClassTypeState();
            }

            return _instance;
        }
    }

    public override void EnterState(Student _owner)
    {
        Debug.Log("Entering New Classroom Type State");
    }

    public override void ExitState(Student _owner)
    {
        Debug.Log("Exiting New Classroom Type State");
    }

    public override void UpdateState(Student _owner)
    {

    }
}