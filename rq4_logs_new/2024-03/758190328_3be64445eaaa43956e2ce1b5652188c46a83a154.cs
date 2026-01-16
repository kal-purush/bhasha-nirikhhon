namespace Tokengram.Utils
{
    public class CosineSimilarityUtil
    {
        public static double GetCosineSimilarity(Dictionary<string, int> userVector, Dictionary<string, int> nftVector)
        {
            // assuming that we want to have keys from user and keys from NFT
            string[] keys = userVector.Keys.Union(nftVector.Keys).Distinct().ToArray();

            double[] vector1 = PrepareAndNormalizeVector(userVector, keys);
            double[] vector2 = PrepareAndNormalizeVector(nftVector, keys);

            return Calculate(vector1, vector2);
        }

        private static double Calculate(double[] vector1, double[] vector2)
        {
            if (vector1.Length != vector2.Length)
                throw new ArgumentException("Vectors must be of the same length.");

            // Calculate dot product
            double dotProduct = vector1.Zip(vector2, (a, b) => a * b).Sum();

            // Calculate magnitudes
            double magnitude1 = Math.Sqrt(vector1.Sum(x => x * x));
            double magnitude2 = Math.Sqrt(vector2.Sum(x => x * x));

            // Calculate cosine similarity
            double cosineSimilarity = dotProduct / (magnitude1 * magnitude2);

            return cosineSimilarity;
        }

        private static double[] PrepareAndNormalizeVector(Dictionary<string, int> vector, string[] keys)
        {
            double[] preparedVector = new double[keys.Length];

            // Prepare vector
            for (int i = 0; i < keys.Length; i++)
                preparedVector[i] = vector.GetValueOrDefault(keys[i]); // 0 if key does not exist

            // Normalize vector to sum up to 100
            double sum = preparedVector.Sum();
            if (sum != 0)
            {
                for (int i = 0; i < preparedVector.Length; i++)
                    preparedVector[i] = Math.Round(preparedVector[i] * 100 / sum, 2);
            }

            return preparedVector;
        }
    }
}