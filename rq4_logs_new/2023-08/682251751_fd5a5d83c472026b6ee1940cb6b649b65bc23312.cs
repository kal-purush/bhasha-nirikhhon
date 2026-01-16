using System;
using Microsoft.ML.Data;

namespace SKKClassifier
{
	public class DataRecord
	{
		private string sensorId;

        private List<float> TempSeq { get; set; } = new List<float>();
        private List<float> LightSeq { get; set; } = new List<float>();
        private List<DateTime> DateSeq { get; set; } = new List<DateTime>();

        [ColumnName("Target")]
		public string Target { get; set; } = "";

        [ColumnName("Temperature")]
        [VectorType(1000)]
        public float[] TempArray
		{
			get
			{
				return TempSeq.ToArray();
			}
		}

        [ColumnName("Features")]
        [VectorType(7)]
        public float[] Features
        {
            get
            {
                return new float[] {
                    Mean(TempSeq), Variance(TempSeq), Length(TempSeq),
                    Min(TempSeq), Max(TempSeq),
                    Positives(LightSeq), // the number of non-zero values in light sequence
                    (float)(DateSeq.Max() - DateSeq.Min()).TotalMinutes // total time span in minutes
                };
            }
        }

        public string SensorId
		{
			get
			{
				return sensorId;
			}
		}

		public DataRecord(string id, List<float> lightSeq, List<float> tempSeq, List<DateTime> dateSeq)
		{
			sensorId = id;
            DateSeq = dateSeq;
            TempSeq = tempSeq;
            LightSeq = lightSeq;
		}

        private float Min(List<float> seq)
        {
            return seq.Min();
        }

        private float Max(List<float> seq)
        {
            return seq.Min();
        }

        private float Mean(List<float> seq)
		{
			return seq.Average();
		}

        private float Length(List<float> seq)
        {
            return seq.Count();
        }

        private float Width(List<float> seq)
        {
            return Max(seq) - Min(seq);
        }

        private float Variance(List<float> seq)
        {
			float var = 0;
			float mean = Mean(seq);
			foreach (float item in seq)
			{
				var += (item - mean) * (item - mean);
			}
            return (float)Math.Sqrt(var) / Length(seq);
        }

        private float Positives(List<float> seq)
        {
            return seq.Count(x => x > 0);
        }

    }


    /// <summary>
    /// Classifier prediction
    /// </summary>
	public class Prediction
	{
        [ColumnName("PredictedLabel")]
        public string Target = "";
    }
}
