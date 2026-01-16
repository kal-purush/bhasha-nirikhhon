using HandwritingNeuralNetwork.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace HandwritingNeuralNetwork.Models
{
    public class TrainingData16 : model_base
    {
        public int ID_TrainingData16 { get; set; }
        public byte[] InputData { get; set; }  //32‑byte packed 16×16 grid
        public char DataLabel { get; set; }  //'0'–'9' or 'n'
        public DateTime LastUpdated { get; set; }

        public TrainingData16() { }
        public TrainingData16(int id) : base(id) { }

        public List<TrainingData16> SelectAll()
            => SelectWhereOrderBy("", "").Cast<TrainingData16>().ToList();

        public int[] TrainingDataCount()
        {
            string sql = "SELECT DataLabel, COUNT(*) AS RecordCount FROM TrainingData GROUP BY DataLabel";

            DataTable dt = ExecuteSQLAsDataTable(sql);

            int[] counts = new int[11];

            foreach (DataRow row in dt.Rows)
            {
                string label = row["DataLabel"].ToString().Trim().ToLower();
                int recordCount = Convert.ToInt32(row["RecordCount"]);

                if (label == "n")
                {
                    counts[10] = recordCount;
                }
                else if (int.TryParse(label, out int digit) && digit >= 0 && digit <= 9)
                {
                    counts[digit] = recordCount;
                }
                else
                {
                    throw new Exception($"Invaild Data Label Returned: {label}");
                }
            }

            return counts;
        }


        ///<summary>
        ///Converts a 16×16 bool matrix into a 32‑byte array (little‑endian per row).
        ///</summary>
        public static byte[] PackBoolMatrix(bool[,] matrix)
        {
            if (matrix == null)
                throw new ArgumentNullException(nameof(matrix));
            if (matrix.GetLength(0) != 16 || matrix.GetLength(1) != 16)
                throw new ArgumentException("Matrix must be exactly 16×16.", nameof(matrix));

            var result = new byte[32];
            for (int r = 0; r < 16; r++)
            {
                ushort rowBits = 0;
                for (int c = 0; c < 16; c++)
                    if (matrix[r, c])
                        rowBits |= (ushort)(1 << c);

                int idx = r * 2;
                result[idx] = (byte)(rowBits & 0xFF);
                result[idx + 1] = (byte)((rowBits >> 8) & 0xFF);
            }
            return result;
        }

        ///<summary>
        ///Unpacks a 32‑byte array back into a 16×16 bool matrix.
        ///</summary>
        public static bool[,] UnpackBoolMatrix(byte[] data)
        {
            if (data == null)
                throw new ArgumentNullException(nameof(data));
            if (data.Length != 32)
                throw new ArgumentException("InputData must be exactly 32 bytes.", nameof(data));

            var matrix = new bool[16, 16];
            for (int r = 0; r < 16; r++)
            {
                int idx = r * 2;
                ushort rowBits = (ushort)(data[idx] | (data[idx + 1] << 8));
                for (int c = 0; c < 16; c++)
                    matrix[r, c] = ((rowBits >> c) & 1) == 1;
            }
            return matrix;
        }
    }
}