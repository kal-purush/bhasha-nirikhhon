using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace HandwritingNeuralNetwork.Models
{
    public class TrainingData : model_base
    {
        public int ID_TrainingData { get; set; }
        public bool[] Data { get; set; }
        public int Score { get; set; }

        public TrainingData() { }
        public TrainingData(int id) : base(id) { }

        public string BoolArrayToString()
        {
            if (Data == null)
                return string.Empty;
            return string.Join("", Array.ConvertAll(Data, b => b ? "1" : "0"));
        }

        
        public static List<TrainingData> LoadAll()
        {
            List<TrainingData> dataList = new List<TrainingData>();
            string tableName = nameof(TrainingData); 
            string sql = $"SELECT * FROM {tableName}";

            string connectionString = $"Server=localhost\\HWNN;Database=HWNN;User Id=sa;Password=abc123!@#;";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                using (SqlCommand command = new SqlCommand(sql, connection))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            TrainingData td = new TrainingData();
                            
                            td.LoadPropertiesFromReader(reader);
                            dataList.Add(td);
                        }
                    }
                }
            }
            return dataList;
        }

    
    }
}
