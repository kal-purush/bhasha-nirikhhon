using Suhdo.StateMachineCore;

namespace Suhdo.Player
{
    public class PlayerAirDashState : PlayerState
    {
        public PlayerAirDashState(StateMachine stateMachine, Entity entity, string animBoolName, PlayerData data)
            : base(stateMachine, entity, animBoolName, data)
        {
        }

        public override void Enter()
        {
            base.Enter();
            
            player.InputHandler.UserDoubleJumpInput();
            Movement.SetVelocityY(playerData.airDashSpeed);
        }

        public override void LogicUpdate()
        {
            base.LogicUpdate();
        }

        public override void PhysicsUpdate()
        {
            base.PhysicsUpdate();
            Movement.SetVelocityY(playerData.airDashSpeed);
        }
    }
}