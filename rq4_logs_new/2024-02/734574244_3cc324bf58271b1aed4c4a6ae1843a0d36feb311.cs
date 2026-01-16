using Suhdo.CharacterCore;
using Suhdo.Weapons.Components.ComponentData;

namespace Suhdo.Weapons.Components
{
	public class Movement : WeaponComponent
	{
		private Suhdo.Movement coreMovement;
		private Suhdo.Movement CoreMovement => coreMovement ??= Core.GetCoreComponent<Suhdo.Movement>();
		private MovementData _data;

		protected override void Awake()
		{
			base.Awake();

			_data = weapon.Data.GetData<MovementData>();
		}

		protected override void OnEnable()
		{
			base.OnEnable();

			eventHandler.OnStartMovement += HandleStartMovement;
			eventHandler.OnStopMovement += HandleStopMovement;
		}

		protected override void OnDisable()
		{
			base.OnDisable();
			
			eventHandler.OnStartMovement -= HandleStartMovement;
			eventHandler.OnStopMovement -= HandleStopMovement;
		}

		private void HandleStartMovement()
		{
			var movementData = _data.AttackData[weapon.CurrentAttackCounter];
			CoreMovement.SetVelocity(movementData.Velocity, movementData.Direction, CoreMovement.FacingDirection);
		}

		private void HandleStopMovement()
		{
			CoreMovement.SetVelocityZero();
		}
	}
}