public class ApeTavern
{
	public static List<long> AllowList { get; } = new()
	{
		76561197990964582, // PUKES
		76561197969358147, // Jammie
		76561198045068860, // Trundler
		76561198042325932, // Kidd
		76561197990720321, // ShadowBrain
		76561198063494192, // Gooman
		76561197993568598, // Matt944
		76561199187854801, // eEight
		76561198032406271, // cameron
		76561198057680429, // Saandy
	};

	public static bool IsApe( long steamId )
	{
		return AllowList.Contains( steamId );
	}
}