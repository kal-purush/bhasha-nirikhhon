using System;
namespace GXPEngine
{
	public class Pickup : AnimationSprite
	{
		Player _player;
		PickupType _type;
		public Pickup( Player player, PickupType pickupType, string filename, int columns, int rows ) : base( filename, columns, rows )
		{
			_player = player;
			_type = pickupType;

			switch (pickupType) 
			{
				case PickupType.CHOCO:
					SetFrame( 0 );
					break;
				case PickupType.COCAINE:
                    SetFrame( 0 );
					break;
				case PickupType.ESPRESSO:
                    SetFrame( 0 );
					break;
				case PickupType.REGULAR:
                    SetFrame( 0 );
					break;
			}
		}
	}
}