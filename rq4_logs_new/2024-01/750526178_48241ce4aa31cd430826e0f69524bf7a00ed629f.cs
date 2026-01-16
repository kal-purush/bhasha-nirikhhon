using System;
namespace FactoryPatternExercise2
{
	public static class DataAccessFactory
	{
		public static IDataAccess GetDataAccessType(string databaseType)
		{
			while (databaseType.ToLower() != "list" && databaseType.ToLower() != "sql" && databaseType.ToLower() != "mongo")
			{
                Console.WriteLine("I'm sorry, I didn't understand that input. Please try again:");
                databaseType = Console.ReadLine();
            }

			switch (databaseType.ToLower())
			{
				case "list":
					return new ListDataAccess();
				case "sql":
					return new SQLDataAccess();
				case "mongo":
					return new MongoDataAccess();
				default:
					throw new NotImplementedException();
			}
		}
	}
}
