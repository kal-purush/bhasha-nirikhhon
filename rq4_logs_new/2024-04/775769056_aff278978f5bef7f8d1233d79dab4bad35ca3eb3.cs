using Microsoft.ML.Data;
using System;
using System.Linq;
using static Tensorflow.Keras.Engine.InputSpec;

namespace LagDaemon.YAMUD.MachineLearning.Console
{
    /// <summary>
    /// Class to hold the fixed length feature vector. Used to define the
    /// column names used as output from the custom mapping action,
    /// </summary>
    public class FixedLength
    {
        /// <summary>
        /// This is a fixed length vector designated by VectorType attribute.
        /// </summary>
        [VectorType(Config.FeatureLength)]
        public int[]? Features { get; set; }
    }

}