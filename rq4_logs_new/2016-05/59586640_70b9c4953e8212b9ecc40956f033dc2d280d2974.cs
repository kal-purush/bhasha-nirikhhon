using Spectra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MassSpectrometry
{
    public class DefaultMsDataFile : MsDataFile<DefaultMzSpectrum>
    {
        public DefaultMsDataFile(string filePath, MsDataFileType filetype = MsDataFileType.UnKnown) : base(filePath, filetype)
        {
        }

        public void Add(MsDataScan<DefaultMzSpectrum>[] Scans)
        {
            this.Scans = Scans;
        }

        public override DissociationType GetDissociationType(int spectrumNumber, int msnOrder = 2)
        {
            throw new NotImplementedException();
        }

        public override double GetInjectionTime(int spectrumNumber)
        {
            throw new NotImplementedException();
        }

        public override bool GetIsCentroid(int spectrumNumber)
        {
            throw new NotImplementedException();
        }

        public override MzRange GetIsolationRange(int spectrumNumber)
        {
            throw new NotImplementedException();
        }

        public override double GetIsolationWidth(int spectrumNumber)
        {
            throw new NotImplementedException();
        }

        public override int GetMsnOrder(int spectrumNumber)
        {
            throw new NotImplementedException();
        }

        public override MZAnalyzerType GetMzAnalyzer(int spectrumNumber)
        {
            throw new NotImplementedException();
        }

        public override MzRange GetMzRange(int spectrumNumber)
        {
            throw new NotImplementedException();
        }

        public override Polarity GetPolarity(int spectrumNumber)
        {
            throw new NotImplementedException();
        }

        public override string GetPrecursorID(int spectrumNumber)
        {
            throw new NotImplementedException();
        }

        public override double GetPrecursorIsolationIntensity(int spectrumNumber)
        {
            throw new NotImplementedException();
        }

        public override double GetPrecursorIsolationMz(int spectrumNumber)
        {
            throw new NotImplementedException();
        }

        public override double GetPrecursorMonoisotopicMz(int spectrumNumber)
        {
            throw new NotImplementedException();
        }

        public override int GetPrecusorCharge(int spectrumNumber, int msnOrder = 2)
        {
            throw new NotImplementedException();
        }

        public override double GetResolution(int spectrumNumber)
        {
            throw new NotImplementedException();
        }

        public override double GetRetentionTime(int spectrumNumber)
        {
            throw new NotImplementedException();
        }

        public override string GetScanFilter(int spectrumNumber)
        {
            throw new NotImplementedException();
        }

        public override DefaultMzSpectrum GetSpectrum(int spectrumNumber)
        {
            throw new NotImplementedException();
        }

        public override string GetSpectrumID(int spectrumNumber)
        {
            throw new NotImplementedException();
        }

        public override int GetSpectrumNumber(double retentionTime)
        {
            throw new NotImplementedException();
        }

        protected override int GetFirstSpectrumNumber()
        {
            throw new NotImplementedException();
        }

        protected override int GetLastSpectrumNumber()
        {
            throw new NotImplementedException();
        }
    }
}