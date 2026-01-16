using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using RAIN.Action;
using RAIN.Core;

[RAINAction]
public class PourCoffee : RAINAction
{
	GameObject barista;
	AIScript script;
	public override void Start(RAIN.Core.AI ai)
    {
        base.Start(ai);
		barista = GameObject.FindGameObjectWithTag ("AITag");
		script = barista.GetComponent<AIScript> ();
	}

    public override ActionResult Execute(RAIN.Core.AI ai)
    {
		Animator anim = barista.GetComponent<Animator> ();
		RuntimeAnimatorController newAnim = (RuntimeAnimatorController)Resources.Load (
			                                    "Assets/Standard Assets/Character/Adam/Adam_IdleController");
		if (newAnim == null) {
			Debug.Log ("Anim not found");
		}
		anim.runtimeAnimatorController = newAnim;
		return ActionResult.SUCCESS;
    }

    public override void Stop(RAIN.Core.AI ai)
    {
        base.Stop(ai);
    }
}