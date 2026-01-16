using System;

namespace Bands
{
    /// <summary>
    /// Helper class for wrapping a band of functionality around some inner functionality.
    /// </summary>
    /// <typeparam name="T">The type of errand</typeparam>
    public class Band<T> : IBand<T> where T : IErrand
    {
        protected IBand<T> InnerBand;

        /// <summary>
        /// Initialize band around another band.
        /// </summary>
        /// <param name="innerBand">Any band for further wrapping some inner functionality.</param>
        /// <exception cref="ArgumentException">ArgumentException is thrown if <paramref name="innerBand"/> is null</exception>
        public Band(IBand<T> innerBand)
        {
            if (innerBand == null)
                throw new ArgumentException("Supplied parameter innerBand cannot be bull.");

            InnerBand = innerBand;
        }

        /// <summary>
        /// Initialize band around some functionality.
        /// </summary>
        /// <param name="errandRunner">Any function accepting the specified generic constraint as a parameter.</param>
        /// <exception cref="ArgumentException">ArgumentException is thrown if <paramref name="errandRunner"/> is null</exception>
        public Band(Action<T> errandRunner)
        {
            if (errandRunner == null)
                throw new ArgumentException("Supplied parameter errandRunner cannot be null.");

            InnerBand = new InnerMostBand<T>(errandRunner);
        }
        
        public void Run(T errand)
        {
            if (InnerBand == null)
                throw new ApplicationException("Band has no inner band.");

            InnerBand.Run(errand);
        }
    }
}