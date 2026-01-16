using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using HandwritingNeuralNetwork.AIModel; //For NeuralNetwork2

namespace HandwritingNeuralNetwork.Models
{
    public class NNModel : model_base
    {
        public int ModelId { get; set; }
        public string ModelName { get; set; }
        public string Layers { get; set; }   //e.g. "[784,128,64,10]"
        public int InputSize { get; set; }
        public int OutputSize { get; set; }
        public DateTime LastUpdated { get; set; }

        public NNModel() : base() { }

        public NNModel(int id) : base(id) { }

        ///<summary>
        ///Helper method to parse the Layers column (a JSON-like array string) into an int array.
        ///</summary>
        public int[] GetLayersArray()
        {
            //Remove any surrounding brackets and whitespace, then split on commas.
            string trimmed = Layers.Trim('[', ']');
            return trimmed.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                          .Select(s => int.Parse(s.Trim()))
                          .ToArray();
        }

        ///<summary>
        ///Loads the NeuralNetwork2 object associated with this model.
        ///It queries the biases and weights tables and then applies those values to a new NeuralNetwork2 instance.
        ///</summary>
        public NeuralNetwork2 LoadNeuralNetwork()
        {
            int[] parsedLayers = GetLayersArray();
            NeuralNetwork2 nn = new NeuralNetwork2(parsedLayers);

            //--- Load Biases ---
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                string sql = "SELECT * FROM NNBiases WHERE ModelId = @ModelId";
                using (SqlCommand cmd = new SqlCommand(sql, connection))
                {
                    cmd.Parameters.AddWithValue("@ModelId", ModelId);
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            //Columns: BiasId, ModelId, LayerIndex, NeuronIndex, BiasValue
                            int layerIndex = (int)reader["LayerIndex"]; //SQL stores 1 for first hidden layer, etc.
                            int neuronIndex = (int)reader["NeuronIndex"];
                            double biasValue = (double)reader["BiasValue"];

                            //Since the NeuralNetwork2.biases list is zero-indexed,
                            //use (layerIndex - 1) as the index.
                            nn.SetBias(layerIndex - 1, neuronIndex, biasValue);
                        }
                    }
                }
            }

            //--- Load Weights ---
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                string sql = "SELECT * FROM NeuralNetworkWeights WHERE ModelId = @ModelId";
                using (SqlCommand cmd = new SqlCommand(sql, connection))
                {
                    cmd.Parameters.AddWithValue("@ModelId", ModelId);
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            //Columns: WeightId, ModelId, FromLayerIndex, ToLayerIndex, RowIndex, ColIndex, WeightValue
                            int toLayerIndex = (int)reader["ToLayerIndex"];
                            int rowIndex = (int)reader["RowIndex"];
                            int colIndex = (int)reader["ColIndex"];
                            double weightValue = (double)reader["WeightValue"];

                            //The weight matrix list in NeuralNetwork2 is zero-indexed,
                            //so use (toLayerIndex - 1) as the matrix index.
                            nn.SetWeight(toLayerIndex - 1, rowIndex, colIndex, weightValue);
                        }
                    }
                }
            }

            return nn;
        }

        ///<summary>
        ///Saves the current NeuralNetwork2 object's parameters (biases and weights)
        ///into the database tables for this model.
        ///</summary>
        public bool SaveNeuralNetwork(NeuralNetwork2 nn)
        {
            bool success = true;
            try
            {
                using (SqlConnection connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    //First, remove any previously saved parameters for this model.
                    using (SqlCommand cmd = new SqlCommand("DELETE FROM NNBiases WHERE ModelId = @ModelId", connection))
                    {
                        cmd.Parameters.AddWithValue("@ModelId", ModelId);
                        cmd.ExecuteNonQuery();
                    }
                    using (SqlCommand cmd = new SqlCommand("DELETE FROM NeuralNetworkWeights WHERE ModelId = @ModelId", connection))
                    {
                        cmd.Parameters.AddWithValue("@ModelId", ModelId);
                        cmd.ExecuteNonQuery();
                    }

                    //--- Save Biases ---
                    //NeuralNetwork2 stores biases as a List<double[]>.
                    for (int layer = 0; layer < nn.Biases.Count; layer++)
                    {
                        double[] biasArray = nn.Biases[layer];
                        for (int neuron = 0; neuron < biasArray.Length; neuron++)
                        {
                            using (SqlCommand cmd = new SqlCommand(
                                "INSERT INTO NNBiases (ModelId, LayerIndex, NeuronIndex, BiasValue) " +
                                "VALUES (@ModelId, @LayerIndex, @NeuronIndex, @BiasValue)", connection))
                            {
                                cmd.Parameters.AddWithValue("@ModelId", ModelId);
                                //Convert back to 1-based layer indexing.
                                cmd.Parameters.AddWithValue("@LayerIndex", layer + 1);
                                cmd.Parameters.AddWithValue("@NeuronIndex", neuron);
                                cmd.Parameters.AddWithValue("@BiasValue", biasArray[neuron]);
                                cmd.ExecuteNonQuery();
                            }
                        }
                    }

                    //--- Save Weights ---
                    //NeuralNetwork2 stores weights as a List<double[,]>
                    for (int layer = 0; layer < nn.Weights.Count; layer++)
                    {
                        double[,] weightMatrix = nn.Weights[layer];
                        int rows = weightMatrix.GetLength(0);
                        int cols = weightMatrix.GetLength(1);
                        // Here, fromLayerIndex equals the current layer, and toLayerIndex equals (layer + 1)
                        int fromLayerIndex = layer;
                        int toLayerIndex = layer + 1;
                        for (int i = 0; i < rows; i++)
                        {
                            for (int j = 0; j < cols; j++)
                            {
                                using (SqlCommand cmd = new SqlCommand(
                                    "INSERT INTO NeuralNetworkWeights (ModelId, FromLayerIndex, ToLayerIndex, RowIndex, ColIndex, WeightValue) " +
                                    "VALUES (@ModelId, @FromLayerIndex, @ToLayerIndex, @RowIndex, @ColIndex, @WeightValue)", connection))
                                {
                                    cmd.Parameters.AddWithValue("@ModelId", ModelId);
                                    cmd.Parameters.AddWithValue("@FromLayerIndex", fromLayerIndex);
                                    cmd.Parameters.AddWithValue("@ToLayerIndex", toLayerIndex);
                                    cmd.Parameters.AddWithValue("@RowIndex", i);
                                    cmd.Parameters.AddWithValue("@ColIndex", j);
                                    cmd.Parameters.AddWithValue("@WeightValue", weightMatrix[i, j]);
                                    cmd.ExecuteNonQuery();
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.Print(ex.ToString());
                success = false;
            }
            return success;
        }
    }
}