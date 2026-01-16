using Terraria.ModLoader;

namespace Specializations
{
	class Specializations : Mod
	{
		public Specializations()
		{
			Properties = new ModProperties()
			{
				Autoload = true,
				AutoloadGores = true,
				AutoloadSounds = true
			};
		}
	}
}