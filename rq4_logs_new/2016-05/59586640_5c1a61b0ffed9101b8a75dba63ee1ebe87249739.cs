using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Spectra
{
    public class DefaultMzSpectrum : MzSpectrum<MzPeak, DefaultMzSpectrum>
    {
        public DefaultMzSpectrum() : base()
        {
        }

        public DefaultMzSpectrum(DefaultMzSpectrum MZSpectrum) : base(MZSpectrum)
        {
        }

        public DefaultMzSpectrum(double[] mz, double[] intensities, bool shouldCopy = true) : base(mz, intensities, shouldCopy)
        {
        }

        public override DefaultMzSpectrum Clone()
        {
            return new DefaultMzSpectrum(this);
        }

        public override DefaultMzSpectrum CorrectMasses(Func<double, double> convertor)
        {
            DefaultMzSpectrum newSpectrum = Clone();
            for (int i = 0; i < newSpectrum.Count; i++)
                newSpectrum.Masses[i] = convertor(newSpectrum.Masses[i]);
            return newSpectrum;
        }

        public override DefaultMzSpectrum Extract(double minMZ, double maxMZ)
        {
            double[] mz;
            double[] intensity;
            ExtractProtected(minMZ, maxMZ, out mz, out intensity);
            return new DefaultMzSpectrum(mz, intensity, false);
        }

        public override DefaultMzSpectrum FilterByIntensity(double minIntensity = 0, double maxIntensity = double.MaxValue)
        {
            double[] mz;
            double[] intensity;
            FilterByIntensityProtected(minIntensity, maxIntensity, out mz, out intensity);
            return new DefaultMzSpectrum(mz, intensity, false);
        }

        public override DefaultMzSpectrum FilterByMZ(IEnumerable<IRange<double>> mzRanges)
        {
            double[] mz;
            double[] intensity;
            FilterByMZProtected(mzRanges, out mz, out intensity);
            return new DefaultMzSpectrum(mz, intensity, false);
        }

        public override DefaultMzSpectrum FilterByMZ(double minMZ, double maxMZ)
        {
            double[] mz;
            double[] intensities;
            FilterByMZProtected(minMZ, maxMZ, out mz, out intensities);
            return new DefaultMzSpectrum(mz, intensities, false);
        }

        public override DefaultMzSpectrum FilterByNumberOfMostIntense(int topNPeaks)
        {
            double[] mz;
            double[] intensities;
            FilterByNumberOfMostIntenseProtected(topNPeaks, out mz, out intensities);
            return new DefaultMzSpectrum(mz, intensities, false);
        }

        public override MzPeak GetPeak(int index)
        {
            return new MzPeak(Masses[index], Intensities[index]);
        }
    }
}