// Copyright 2012, 2013, 2014 Derek J. Bailey
// Modified work copyright 2016 Stefan Solntsev
// 
// This file (MZSpectrum.cs) is part of MassSpectrometry.
// 
// MassSpectrometry is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// MassSpectrometry is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
// License for more details.
// 
// You should have received a copy of the GNU Lesser General Public
// License along with MassSpectrometry. If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

namespace Spectra
{
    public abstract class Spectrum<TPeak, TSpectrum> : ISpectrum<TPeak>
        where TPeak : Peak
        where TSpectrum : Spectrum<TPeak, TSpectrum>
    {
        /// <summary>
        /// The m/z of this spectrum in ascending order
        /// </summary>
        protected double[] Masses;

        /// <summary>
        /// The intensity of this spectrum, linked to their m/z by index in the array
        /// </summary>
        protected double[] Intensities;

        /// <summary>
        /// The number of peaks in this spectrum
        /// </summary>
        public int Count { get; protected set; }

        /// <summary>
        /// The first m/z of this spectrum
        /// </summary>
        public double FirstMZ
        {
            get { return Masses[0]; }
        }

        /// <summary>
        /// The last m/z of this spectrum
        /// </summary>
        public double LastMZ
        {
            get { return Masses[Count - 1]; }
        }

        /// <summary>
        /// The total ion current of this spectrum
        /// </summary>
        public double TotalIonCurrent
        {
            get { return GetTotalIonCurrent(); }
        }

        public TPeak this[int index]
        {
            get
            {
                return GetPeak(index);
            }
        }

        #region constructors


        /// <summary>
        /// Initializes a new spectrum
        /// </summary>
        /// <param name="mz">The m/z's</param>
        /// <param name="intensities">The intensities</param>
        /// <param name="shouldCopy">Indicates whether the input arrays should be copied to new ones</param>
        public Spectrum(double[] mz, double[] intensities, bool shouldCopy = true)
        {
            Count = mz.Length;
            Masses = CopyData(mz, shouldCopy);
            Intensities = CopyData(intensities, shouldCopy);
        }

        /// <summary>
        /// Initializes a new spectrum from another spectrum
        /// </summary>
        /// <param name="MZSpectrum">The spectrum to clone</param>
        public Spectrum(TSpectrum MZSpectrum)
            : this(MZSpectrum.Masses, MZSpectrum.Intensities)
        {
        }

        protected Spectrum()
        {
            Count = 0;
            Masses = new double[0];
            Intensities = new double[0];
        }

        /// <summary>
        /// Initializes a new spectrum from another spectrum
        /// </summary>
        /// <param name="spectrum">The spectrum to clone</param>
        private Spectrum(ISpectrum<Peak> spectrum)
            : this(spectrum.GetMasses(), spectrum.GetIntensities())
        {
        }

        /// <summary>
        /// Initializes a new spectrum
        /// </summary>
        /// <param name="mzintensities"></param>
        private Spectrum(double[,] mzintensities)
            : this(mzintensities, mzintensities.GetLength(1))
        {
        }

        public Spectrum(double[,] mzintensities, int count)
        {
            int length = mzintensities.GetLength(1);

            Masses = new double[count];
            Intensities = new double[count];
            Buffer.BlockCopy(mzintensities, 0, Masses, 0, sizeof(double) * count);
            Buffer.BlockCopy(mzintensities, sizeof(double) * length, Intensities, 0, sizeof(double) * count);
            Count = count;
        }

        private Spectrum(byte[] mzintensities)
        {
            Count = mzintensities.Length / (sizeof(double) * 2);
            int size = sizeof(double) * Count;
            Masses = new double[Count];
            Intensities = new double[Count];
            Buffer.BlockCopy(mzintensities, 0, Masses, 0, size);
            Buffer.BlockCopy(mzintensities, size, Intensities, 0, size);
        }

        #endregion

        public abstract TPeak GetPeak(int index);

        /// <summary>
        /// Finds the largest intensity value in this spectrum
        /// </summary>
        /// <returns>The largest intensity value in this spectrum</returns>
        public double GetBasePeakIntensity()
        {
            return Intensities.Max();
        }

        /// <summary>
        /// Gets the full m/z range of this spectrum
        /// </summary>
        /// <returns></returns>
        public MzRange GetMzRange()
        {
            return new MzRange(FirstMZ, LastMZ);
        }

        /// <summary>
        /// Gets a copy of the underlying m/z array
        /// </summary>
        /// <returns></returns>
        public double[] GetMasses()
        {
            return CopyData(Masses);
        }

        /// <summary>
        /// Gets a copy of the underlying intensity array
        /// </summary>
        /// <returns></returns>
        public double[] GetIntensities()
        {
            return CopyData(Intensities);
        }

        /// <summary>
        /// Converts the spectrum into a multi-dimensional array of doubles
        /// </summary>
        /// <returns></returns>
        public virtual double[,] ToArray()
        {
            double[,] data = new double[2, Count];
            const int size = sizeof(double);
            Buffer.BlockCopy(Masses, 0, data, 0, size * Count);
            Buffer.BlockCopy(Intensities, 0, data, size * Count, size * Count);
            return data;
        }

        /// <summary>
        /// Calculates the total ion current of this spectrum
        /// </summary>
        /// <returns>The total ion current of this spectrum</returns>
        public double GetTotalIonCurrent()
        {
            return Intensities.Sum();
        }

        /// <summary>
        /// Gets the m/z value at the specified index
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public double GetMass(int index)
        {
            return Masses[index];
        }

        /// <summary>
        /// Gets the intensity value at the specified index
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public double GetIntensity(int index)
        {
            return Intensities[index];
        }

        /// <summary>
        /// Checks if this spectrum contains any peaks
        /// </summary>
        /// <returns></returns>
        public bool ContainsAnyPeaks()
        {
            return Count > 0;
        }

        /// <summary>
        /// Checks if this spectrum contains any peaks within the range
        /// </summary>
        /// <param name="range"></param>
        /// <returns></returns>
        public bool ContainsAnyPeaksWithinRange(IRange<double> range)
        {
            return ContainsPeak(range.Minimum, range.Maximum);
        }

        /// <summary>
        /// Checks if this spectrum contains any peaks within the range
        /// </summary>
        /// <param name="minMZ">The minimum m/z (inclusive)</param>
        /// <param name="maxMZ">The maximum m/z (inclusive)</param>
        /// <returns></returns>
        public bool ContainsPeak(double minMZ, double maxMZ)
        {
            if (Count == 0)
                return false;

            int index = Array.BinarySearch(Masses, minMZ);
            if (index >= 0)
                return true;

            index = ~index;

            return index < Count && Masses[index] <= maxMZ;
        }

        public TPeak GetBasePeak()
        {
            return GetPeak(Intensities.MaxIndex());
        }

        public TPeak GetClosestPeak(IRange<double> massRange)
        {
            double mean = (massRange.Maximum + massRange.Minimum) / 2.0;
            return GetClosestPeak(mean);
        }

        public TPeak GetClosestPeak(double mean)
        {
            int index = GetClosestPeakIndex(mean);
            return GetPeak(index);
        }

        public string ToBase64String(bool zlibCompressed = false)
        {
            return Convert.ToBase64String(ToBytes(zlibCompressed));
        }

        public virtual byte[] ToBytes(bool zlibCompressed = false)
        {
            return ToBytes(zlibCompressed, Masses, Intensities);
        }

        public abstract TSpectrum Clone();

        /// <summary>
        /// Creates a clone of this spectrum with each mass transformed by some function
        /// </summary>
        /// <param name="convertor">The function to convert each mass by</param>
        /// <returns>A cloned spectrum with masses corrected</returns>
        public abstract TSpectrum CorrectMasses(Func<double, double> convertor);


        #region private Methods

        private double[] FromBytes(byte[] data, int index)
        {
            if (data.IsCompressed())
                data = data.Decompress();
            Count = data.Length / (sizeof(double) * 2);
            int size = sizeof(double) * Count;
            double[] outArray = new double[Count];
            Buffer.BlockCopy(data, index * size, outArray, 0, size);
            return outArray;
        }

        protected byte[] ToBytes(bool zlibCompressed, params double[][] arrays)
        {
            int length = Count * sizeof(double);
            int arrayCount = arrays.Length;
            byte[] bytes = new byte[length * arrayCount];
            int i = 0;
            foreach (double[] array in arrays)
            {
                Buffer.BlockCopy(array, 0, bytes, length * i++, length);
            }

            if (zlibCompressed)
            {
                bytes = bytes.Compress();
            }

            return bytes;
        }

        /// <summary>
        /// Copies the source array to the destination array
        /// </summary>
        /// <typeparam name="TArray"></typeparam>
        /// <param name="sourceArray">The source array to copy from</param>
        /// <param name="deepCopy">If true, a new array will be generate, else references are copied</param>
        protected TArray[] CopyData<TArray>(TArray[] sourceArray, bool deepCopy = true) where TArray : struct
        {
            if (sourceArray == null)
                return null;
            if (sourceArray.Length != Count)
                throw new ArgumentException("Mismatched array size");
            if (!deepCopy)
            {
                return sourceArray;
            }
            int count = sourceArray.Length;
            TArray[] dstArray = new TArray[Count];
            Type type = typeof(TArray);
            Buffer.BlockCopy(sourceArray, 0, dstArray, 0, count * Marshal.SizeOf(type));
            return dstArray;
        }

        private int GetClosestPeakIndex(double meanMZ)
        {
            if (Count == 0)
                return -1;

            int index = Array.BinarySearch(Masses, meanMZ);
            if (index >= 0)
                return index;
            index = ~index;

            int indexm1 = index - 1;

            if (index >= Count)
            {
                // only the indexm1 peak can be closer

                if (indexm1 >= 0)
                {
                    return indexm1;
                }
                return -1;
            }
            if (index == 0)
            {
                // only the index can be closer
                return index;
            }

            double p1 = Masses[indexm1];
            double p2 = Masses[index];

            if (meanMZ - p1 > p2 - meanMZ)
                return index;
            return indexm1;
        }

        protected int GetPeakIndex(double mz)
        {
            int index = Array.BinarySearch(Masses, mz);

            if (index >= 0)
                return index;

            return ~index;
        }

        #endregion private Methods

        public override string ToString()
        {
            return string.Format("{0} (Peaks {1})", GetMzRange(), Count);
        }

        public IEnumerator<TPeak> GetEnumerator()
        {
            for (int i = 0; i < Count; i++)
            {
                yield return GetPeak(i);
            }
        }

        protected void ExtractProtected(double minMZ, double maxMZ, out double[] mz, out double[] intensity)
        {
            int index = GetPeakIndex(minMZ);

            int count = Count;
            mz = new double[count];
            intensity = new double[count];
            int j = 0;

            while (index < Count && Masses[index] <= maxMZ)
            {
                mz[j] = Masses[index];
                intensity[j] = Intensities[index];
                index++;
                j++;
            }

            Array.Resize(ref mz, j);
            Array.Resize(ref intensity, j);
        }

        protected void FilterByIntensityProtected(double minIntensity, double maxIntensity, out double[] mz, out double[] intensities)
        {
            int count = Count;
            mz = new double[count];
            intensities = new double[count];
            int j = 0;
            for (int i = 0; i < count; i++)
            {
                double intensity = Intensities[i];
                if (intensity >= minIntensity && intensity < maxIntensity)
                {
                    mz[j] = Masses[i];
                    intensities[j] = intensity;
                    j++;
                }
            }


            if (j != count)
            {
                Array.Resize(ref mz, j);
                Array.Resize(ref intensities, j);
            }
        }

        protected void FilterByMZProtected(IEnumerable<IRange<double>> mzRanges, out double[] mz, out double[] intensities)
        {

            int count = Count;

            // Peaks to remove
            HashSet<int> indiciesToRemove = new HashSet<int>();

            // Loop over each range to remove
            foreach (IRange<double> range in mzRanges)
            {
                double min = range.Minimum;
                double max = range.Maximum;

                int index = Array.BinarySearch(Masses, min);
                if (index < 0)
                    index = ~index;

                while (index < count && Masses[index] <= max)
                {
                    indiciesToRemove.Add(index);
                    index++;
                }
            }

            // The size of the cleaned spectrum
            int cleanCount = count - indiciesToRemove.Count;


            // Create the storage for the cleaned spectrum
            mz = new double[cleanCount];
            intensities = new double[cleanCount];

            // Transfer peaks from the old spectrum to the new one
            int j = 0;
            for (int i = 0; i < count; i++)
            {
                if (indiciesToRemove.Contains(i))
                    continue;
                mz[j] = Masses[i];
                intensities[j] = Intensities[i];
                j++;
            }

        }

        protected void FilterByMZProtected(double minMZ, double maxMZ, out double[] mz, out double[] intensities)
        {
            int count = Count;

            // Peaks to remove
            HashSet<int> indiciesToRemove = new HashSet<int>();

            int index = Array.BinarySearch(Masses, minMZ);
            if (index < 0)
                index = ~index;

            while (index < count && Masses[index] <= maxMZ)
            {
                indiciesToRemove.Add(index);
                index++;
            }


            // The size of the cleaned spectrum
            int cleanCount = count - indiciesToRemove.Count;

            // Create the storage for the cleaned spectrum
            mz = new double[cleanCount];
            intensities = new double[cleanCount];

            // Transfer peaks from the old spectrum to the new one
            int j = 0;
            for (int i = 0; i < count; i++)
            {
                if (indiciesToRemove.Contains(i))
                    continue;
                mz[j] = Masses[i];
                intensities[j] = Intensities[i];
                j++;
            }
        }

        protected void FilterByNumberOfMostIntenseProtected(int topNPeaks, out double[] mz, out double[] intensities)
        {
            mz = new double[topNPeaks];
            intensities = new double[topNPeaks];

            IComparer<double> mycomparer = new ReverseComparer();

            Array.Sort(Intensities, Masses, mycomparer);

            mz = Masses.SubArray(0, topNPeaks);
            intensities = Intensities.SubArray(0, topNPeaks);

        }

        public abstract TSpectrum Extract(double minMZ, double maxMZ);

        public abstract TSpectrum FilterByIntensity(double minIntensity = 0, double maxIntensity = double.MaxValue);

        public abstract TSpectrum FilterByNumberOfMostIntense(int topNPeaks);

        public abstract TSpectrum FilterByMZ(IEnumerable<IRange<double>> mzRanges);

        public abstract TSpectrum FilterByMZ(double minMZ, double maxMZ);

        public TSpectrum Extract(IRange<double> mzRange)
        {
            return Extract(mzRange.Minimum, mzRange.Maximum);
        }

        public TSpectrum FilterByMZ(IRange<double> mzRange)
        {
            return FilterByMZ(mzRange.Minimum, mzRange.Maximum);
        }

        public TSpectrum FilterByIntensity(IRange<double> intenistyRange)
        {
            return FilterByIntensity(intenistyRange.Minimum, intenistyRange.Maximum);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        ISpectrum<TPeak> ISpectrum<TPeak>.Extract(IRange<double> mzRange)
        {
            return Extract(mzRange);
        }

        ISpectrum<TPeak> ISpectrum<TPeak>.Extract(double minMZ, double maxMZ)
        {
            return Extract(minMZ, maxMZ);
        }

        ISpectrum<TPeak> ISpectrum<TPeak>.FilterByMZ(IEnumerable<IRange<double>> mzRanges)
        {
            return FilterByMZ(mzRanges);
        }

        ISpectrum<TPeak> ISpectrum<TPeak>.FilterByMZ(IRange<double> mzRange)
        {
            return FilterByMZ(mzRange);
        }

        ISpectrum<TPeak> ISpectrum<TPeak>.FilterByMZ(double minMZ, double maxMZ)
        {
            return FilterByMZ(minMZ, maxMZ);
        }

        ISpectrum<TPeak> ISpectrum<TPeak>.FilterByIntensity(double minIntensity, double maxIntensity)
        {
            return FilterByIntensity(minIntensity, maxIntensity);
        }

        ISpectrum<TPeak> ISpectrum<TPeak>.FilterByIntensity(IRange<double> intenistyRange)
        {
            return FilterByIntensity(intenistyRange);
        }

        ISpectrum<TPeak> ISpectrum<TPeak>.CorrectMasses(Func<double, double> convertor)
        {
            return CorrectMasses(convertor);
        }

        IEnumerator<TPeak> IEnumerable<TPeak>.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    internal class ReverseComparer : IComparer<double>
    {
        public int Compare(double x, double y)
        {
            if (x > y)
                return -1;
            else if (x < y)
                return 1;
            else
                return 0;
        }
    }

}