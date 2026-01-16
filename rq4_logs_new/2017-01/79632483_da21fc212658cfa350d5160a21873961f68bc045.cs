using System.Collections;
using System.Collections.Generic;
using UniRx;
using UniRx.Triggers;
using UnityEngine;
using UnityEngine.Events;

public class StartButton : MonoBehaviour {

    private ObservableTriggerTrigger trigger;

    public UnityEvent HandleStartTriggerEnter;

    void Awake() {
        trigger = GetComponent<ObservableTriggerTrigger>();
    }

	// Use this for initialization
	void Start () {
        trigger.OnTriggerEnterAsObservable().Subscribe(collider => {
            HandleStartTriggerEnter.Invoke();
        }).AddTo(this);
    }
	
	// Update is called once per frame
	void Update () {
		
	}
}