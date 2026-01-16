using Suhdo.StateMachineCore;

namespace Suhdo.Player
{
    public class PlayerAirDashGroundState : PlayerState
    {
        public PlayerAirDashGroundState(StateMachine stateMachine, Entity entity, string animBoolName, PlayerData data)
            : base(stateMachine, entity, animBoolName, data)
        {
        }

        public override void Enter()
        {
            base.Enter();
            
            Movement.SetVelocityZero();
        }

        public override void AnimationFinishTrigger()
        {
            base.AnimationFinishTrigger();
            
            stateMachine.ChangeState(player.IdleState);
        }
    }
}