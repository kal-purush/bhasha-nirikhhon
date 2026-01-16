using UnityEngine;
using System.Collections;
[System.Serializable]
public class StateMachine : MonoBehaviour {



    public virtual State CurrentState
    {
        get { return _currentState; }
        set { Transition(value); }
    }

    protected State _currentState;
    protected bool inTransition;

    public virtual T GetState<T>()where T : State
    {
        T target = GetComponent<T>();
        if (target == null)
            target = gameObject.AddComponent<T>();
        return target;
    }

    public virtual void ChangeState<T>() where T : State
    {
        CurrentState = GetState<T>();
    }

    protected virtual void Transition(State value)
    {
        if (_currentState == value || inTransition)
            return;

        inTransition = true;
        if (_currentState != null)
            _currentState.Exit();

        _currentState = value;
        if (_currentState != null)
            _currentState.Enter();

        inTransition = false;

    }

    // Use this for initialization
    void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}
}