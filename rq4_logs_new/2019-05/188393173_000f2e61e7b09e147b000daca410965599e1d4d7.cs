namespace Amaze
{
	public static class Settings
	{
		/// <summary>
		/// will see as failed if path node count reach this limitation
		/// </summary>
		public const int MAX_PATH_NODE_COUNT = 100;

		/// <summary>
		/// at the same time search path limitations
		/// </summary>
		public const int MAX_EXECUTE_PATH_COUNT = 5000;

		/// <summary>
		/// end the search if solved path reach this limitation
		/// </summary>
		public const int MAX_SOLVE_COUNT = 100;
	}
}