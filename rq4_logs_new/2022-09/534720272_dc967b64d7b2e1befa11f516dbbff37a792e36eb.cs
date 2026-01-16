using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AttackState : State
{

    public AttackState(StateController stateController) : base(stateController) { }

        public override void CheckTransitions()
        {
            if (!stateController.CheckIfInRange("Player"))
            {
                stateController.SetState(new PatrolState(stateController));
            }

            if(stateController.hasAttacked)
            {
                stateController.SetState(new FleeState(stateController));
            }
        }

        public override void Act()
        {
            if(stateController.enemyToChase != null)
            {
                stateController.destination = stateController.enemyToChase.transform;
                stateController.ai.SetTarget(stateController.destination);
            }
        }

        public override void OnStateEnter()
        {
            stateController.ChangeColor(Color.red);
        }
}