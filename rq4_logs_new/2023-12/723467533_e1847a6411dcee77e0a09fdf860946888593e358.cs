using Microsoft.Data.SqlClient;
using System;

using System.Text;

namespace _03_Minion_Names
{
    public class StartUp
    {
        private const string ConnectionString =
            @"Server=.;Database=MinionDB;Integrated Security=true;";
        public static void Main(string[] args)
        {
            using (SqlConnection sqlConnection = new SqlConnection(
                                       ConnectionString))
            {
                sqlConnection.Open();
                int villainId = int.Parse(Console.ReadLine());

                string result = GetMinionsInfoAboutVillain(sqlConnection, villainId);



                Console.WriteLine(result);
            }
        }

        
        private static string GetMinionsInfoAboutVillain(
               SqlConnection sqlConnection, int villainId)
        {
            StringBuilder sb = new StringBuilder();

            string getVillainNameQueryText =
                          @"SELECT [Name] FROM Villains
                            WHERE Id=@villainId";

            string villainName;

            using (SqlCommand getVillainNameCmd =
                   new SqlCommand(getVillainNameQueryText, sqlConnection))
            {

                getVillainNameCmd.Parameters.AddWithValue(
                             "@villainId", villainId);
                villainName = getVillainNameCmd
                                  .ExecuteScalar()?
                                  .ToString();

                if (villainName == null)
                {
                    sb.AppendLine($"No villain with " +
                        $"ID {villainId} exists in the database.");
                }
                else
                {
                    sb.AppendLine($"Villain: {villainName}");

                }
            }

            // string getMinionsInfoQueryText =
            //     @"SELECT m.[Name],m.[Age] FROM Villains AS v
            //     LEFT OUTER JOIN MinionsVillains AS mv
            //       ON v.Id=mv.VillainId
            //      LEFT OUTER JOIN Minions AS m
            //        ON mv.MinionId=m.Id
            //       WHERE v.[Name]='@villainName'
            //       ORDER BY m.[Name]";

            string getMinionsInfoQueryText =
            @"SELECT ROW_NUMBER() OVER(ORDER BY m.Name) as RowNum,
                                         m.Name, 
                                         m.Age
                                    FROM MinionsVillains AS mv
                                    JOIN Minions As m ON mv.MinionId = m.Id
                                   WHERE mv.VillainId = @vilainId
                                ORDER BY m.Name";


            using SqlCommand getMinionsInfoCommand =
                new SqlCommand(getMinionsInfoQueryText, sqlConnection);

            getMinionsInfoCommand.Parameters
            .AddWithValue("@vilainId", villainId);
                    //  .AddWithValue("@villainName", villainName);

            using SqlDataReader reader = getMinionsInfoCommand.ExecuteReader();

                if(reader.HasRows)
                {
                    int rowNum = 1;
                    while (reader.Read())
                    {
                        string minionName = reader["Name"]?.ToString();
                        string minionAge = reader["Age"]?.ToString();

                        sb.AppendLine($"{rowNum}.{minionName} {minionAge}");
                        rowNum++;
                    }
                }
                else
                {
                    sb.AppendLine("(no minions)");
                }

            

            return sb.ToString().TrimEnd();
        }
    }
}