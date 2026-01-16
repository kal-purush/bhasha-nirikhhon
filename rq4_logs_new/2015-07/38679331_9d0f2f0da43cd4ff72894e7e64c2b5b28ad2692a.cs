using System;

namespace Sithouse
{
	/// <summary>
	/// The interface between all database types. Each format must implement each of these.
	/// </summary>
	public interface ISql
	{
		/// <summary>
		/// The format for use in the properties file
		/// </summary>
		/// <returns>The format name.</returns>
		string GetFormat();

		/* Here im thinking we have an open connection so we aren't always opening connections
		 * The remote sql server could potentially be on the other side of the world
		 */

		bool CloseConnection ();

		/// <summary>
		/// Opens a connection to the sql database
		/// </summary>
		/// <returns><c>true</c>, if connection was opened, <c>false</c> otherwise.</returns>
		bool OpenConnection (string host, int port);

		/// <summary>
		/// Checks to see if our connection is closed. If it's closed then it will reopen
		/// </summary>
		/// <returns>False when no connection, True when a live connection is established</returns>
		bool CheckConnection ();
	}
}
