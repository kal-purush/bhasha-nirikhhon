using UnityEngine;

public class GruzMother_AttackPlayer : StateMachineBehaviour
{
    [SerializeField]
    GruzeMother gruz;
    // OnStateEnter is called when a transition starts and the state machine starts to evaluate this state
    override public void OnStateEnter(Animator animator, AnimatorStateInfo stateInfo, int layerIndex)
    {
        gruz = GameObject.FindGameObjectWithTag("GruzMother").GetComponent<GruzeMother>();
    }

    // OnStateUpdate is called on each Update frame between OnStateEnter and OnStateExit callbacks
    override public void OnStateUpdate(Animator animator, AnimatorStateInfo stateInfo, int layerIndex)
    {
        gruz.AttackPlayer();
    }
}